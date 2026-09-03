package io.dispatchgrid.matching;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/** Readiness: the Streams instance must be RUNNING (or REBALANCING, which is transient). */
@Component("streams")
public class StreamsHealthIndicator implements HealthIndicator {
  private final StreamsBuilderFactoryBean factory;

  public StreamsHealthIndicator(StreamsBuilderFactoryBean factory) {
    this.factory = factory;
  }

  @Override
  public Health health() {
    KafkaStreams streams = factory.getKafkaStreams();
    if (streams == null) {
      return Health.down().withDetail("state", "NOT_STARTED").build();
    }
    KafkaStreams.State state = streams.state();
    boolean ok = state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING;
    return (ok ? Health.up() : Health.down()).withDetail("state", state.name()).build();
  }
}
