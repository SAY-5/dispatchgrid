package io.dispatchgrid.matching;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.RideUnmatched;
import io.dispatchgrid.common.serde.JsonSerde;
import io.dispatchgrid.common.shard.TripRepository;
import io.dispatchgrid.matching.surge.SurgeProperties;
import io.dispatchgrid.matching.surge.SurgeTracker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The request path through the topology with a mocked clock: retries park and replay in order. */
class MatchingTopologyTest {
  static final double LAT = 47.6062;
  static final double LNG = -122.3321;

  @TempDir Path stateDir;

  InMemoryDriverIndex index = new InMemoryDriverIndex();
  MatchingProperties props =
      new MatchingProperties(
          1000,
          2.0,
          8000,
          3,
          Duration.ofSeconds(20),
          new MatchingProperties.Retry(3, Duration.ofSeconds(5), Duration.ofSeconds(1)));
  TripRepository trips = mock(TripRepository.class);
  MatchStats stats = new MatchStats(new SimpleMeterRegistry(), Clock.systemUTC());
  TopologyTestDriver driver;
  TestInputTopic<String, RideRequest> requests;
  TestOutputTopic<String, Match> matches;
  TestOutputTopic<String, RideUnmatched> unmatched;

  @BeforeEach
  void setUp() {
    when(trips.markMatched(any())).thenReturn(true);
    when(trips.markUnmatched(any(), anyInt(), any())).thenReturn(true);
    SurgeTracker surge =
        new SurgeTracker(
            new SurgeProperties(0, null, null, 0, 0, 0),
            new SimpleMeterRegistry(),
            Clock.systemUTC());
    MatchService service =
        new MatchService(
            new Matcher(index, props, surge, Clock.systemUTC()),
            trips,
            index,
            stats,
            props.retry().maxAttempts());
    StreamsBuilder builder = new StreamsBuilder();
    new MatchingTopology().rideRequests(builder, service, surge, props);
    Properties p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "none:9092");
    p.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    driver = new TopologyTestDriver(builder.build(), p);
    requests =
        driver.createInputTopic(
            Topics.RIDE_REQUESTS,
            new StringSerializer(),
            JsonSerde.of(RideRequest.class).serializer());
    matches =
        driver.createOutputTopic(
            Topics.RIDE_MATCHES,
            new StringDeserializer(),
            JsonSerde.of(Match.class).deserializer());
    unmatched =
        driver.createOutputTopic(
            Topics.RIDE_UNMATCHED,
            new StringDeserializer(),
            JsonSerde.of(RideUnmatched.class).deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  private static RideRequest ride(String id) {
    return new RideRequest(id, "rider", 1, LAT, LNG, LAT + 0.01, LNG, Instant.now());
  }

  @Test
  void aRideWaitsForADriverInsteadOfFailingOnAnEmptyCity() {
    requests.pipeInput("1", ride("r1"));
    assertThat(matches.isEmpty()).isTrue();
    assertThat(unmatched.isEmpty()).isTrue();
    assertThat(stats.snapshot()).containsEntry("retries", 1L);

    index.add("d1", 1, LAT + 0.002, LNG);
    driver.advanceWallClockTime(Duration.ofSeconds(4));
    assertThat(matches.isEmpty()).isTrue();

    driver.advanceWallClockTime(Duration.ofSeconds(2));
    Match m = matches.readValue();
    assertThat(m.rideId()).isEqualTo("r1");
    assertThat(m.driverId()).isEqualTo("d1");
    assertThat(unmatched.isEmpty()).isTrue();
    verify(trips).appendEvent(eq("r1"), eq(1), eq("ride.retry"), any());
  }

  @Test
  void givesUpAfterTheLastAttemptAndReportsHowManyItTook() {
    requests.pipeInput("1", ride("r2"));
    driver.advanceWallClockTime(Duration.ofSeconds(6));
    assertThat(unmatched.isEmpty()).isTrue();
    assertThat(stats.snapshot()).containsEntry("retries", 2L);

    driver.advanceWallClockTime(Duration.ofSeconds(11));
    RideUnmatched u = unmatched.readValue();
    assertThat(u.rideId()).isEqualTo("r2");
    assertThat(u.attempts()).isEqualTo(3);
    assertThat(u.reason()).isEqualTo("no_drivers_in_range");
    assertThat(stats.snapshot()).containsEntry("unmatched", 1L);
    verify(trips).markUnmatched(eq("r2"), eq(1), any());
  }
}
