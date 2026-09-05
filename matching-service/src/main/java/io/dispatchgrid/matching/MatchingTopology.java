package io.dispatchgrid.matching;

import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.RideUnmatched;
import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.serde.JsonSerde;
import io.dispatchgrid.common.shard.CityShardRouter;
import io.dispatchgrid.common.shard.TripRepository;
import io.dispatchgrid.matching.surge.SurgeProperties;
import io.dispatchgrid.matching.surge.SurgeTracker;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * ride-requests -> match -> ride-matches | ride-unmatched. Records stay keyed by city id, so all of
 * a city's decisions are made in order on one stream task while cities run in parallel. The
 * driver-positions topic is tapped read-only to keep the per-cell supply side of the surge signal
 * current; requests feed its demand side just before they are matched. ride-lifecycle carries
 * cancellations and completions, which free the driver's claim.
 */
@Configuration
@EnableKafkaStreams
@EnableConfigurationProperties({MatchingProperties.class, SurgeProperties.class})
public class MatchingTopology {

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  public TripRepository tripRepository(CityShardRouter router) {
    return new TripRepository(router);
  }

  @Bean
  public MatchStats matchStats(MeterRegistry registry, Clock clock) {
    return new MatchStats(registry, clock);
  }

  @Bean
  public SurgeTracker surgeTracker(SurgeProperties props, MeterRegistry registry, Clock clock) {
    return new SurgeTracker(props, registry, clock);
  }

  @Bean
  public Matcher matcher(
      DriverIndex index, MatchingProperties props, SurgeTracker surge, Clock clock) {
    return new Matcher(index, props, surge, clock);
  }

  @Bean
  public MatchService matchService(
      Matcher matcher, TripRepository trips, DriverIndex index, MatchStats stats) {
    return new MatchService(matcher, trips, index, stats);
  }

  @Bean
  public LifecycleService lifecycleService(
      TripRepository trips, DriverIndex index, MatchStats stats) {
    return new LifecycleService(trips, index, stats);
  }

  @Bean
  public KStream<String, TripEvent> tripEvents(StreamsBuilder builder, LifecycleService lifecycle) {
    KStream<String, TripEvent> events =
        builder.stream(
            Topics.RIDE_LIFECYCLE, Consumed.with(Serdes.String(), JsonSerde.of(TripEvent.class)));
    events.foreach((city, e) -> lifecycle.handle(e), Named.as("lifecycle"));
    return events;
  }

  @Bean
  public KStream<String, DriverPosition> driverPositions(
      StreamsBuilder builder, SurgeTracker surge) {
    KStream<String, DriverPosition> positions =
        builder.stream(
            Topics.DRIVER_POSITIONS,
            Consumed.with(Serdes.String(), JsonSerde.of(DriverPosition.class)));
    positions.foreach((city, p) -> surge.recordDriver(p), Named.as("surge-supply"));
    return positions;
  }

  @Bean
  public KStream<String, RideRequest> rideRequests(
      StreamsBuilder builder, MatchService service, SurgeTracker surge) {
    KStream<String, RideRequest> requests =
        builder.stream(
            Topics.RIDE_REQUESTS, Consumed.with(Serdes.String(), JsonSerde.of(RideRequest.class)));

    KStream<String, MatchOutcome> outcomes =
        requests
            .peek(
                (city, r) -> surge.recordRequest(r.cityId(), r.pickupLat(), r.pickupLng()),
                Named.as("surge-demand"))
            .flatMapValues(service::handle, Named.as("match"));

    outcomes
        .split(Named.as("outcome-"))
        .branch(
            (city, o) -> o.isMatched(),
            Branched.withConsumer(
                s ->
                    s.mapValues(MatchOutcome::match)
                        .to(
                            Topics.RIDE_MATCHES,
                            Produced.with(Serdes.String(), JsonSerde.of(Match.class)))))
        .defaultBranch(
            Branched.withConsumer(
                s ->
                    s.mapValues(MatchOutcome::unmatched)
                        .to(
                            Topics.RIDE_UNMATCHED,
                            Produced.with(Serdes.String(), JsonSerde.of(RideUnmatched.class)))));
    return requests;
  }
}
