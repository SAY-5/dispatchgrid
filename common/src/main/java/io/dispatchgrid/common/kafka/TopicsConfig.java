package io.dispatchgrid.common.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Declares the four topics so any service can bring up a fresh broker. */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnBean(KafkaAdmin.class)
public class TopicsConfig {

  @Bean
  public NewTopic rideRequestsTopic() {
    return TopicBuilder.name(Topics.RIDE_REQUESTS).partitions(Topics.PARTITIONS).build();
  }

  @Bean
  public NewTopic driverPositionsTopic() {
    return TopicBuilder.name(Topics.DRIVER_POSITIONS).partitions(Topics.PARTITIONS).build();
  }

  @Bean
  public NewTopic rideMatchesTopic() {
    return TopicBuilder.name(Topics.RIDE_MATCHES).partitions(Topics.PARTITIONS).build();
  }

  @Bean
  public NewTopic rideUnmatchedTopic() {
    return TopicBuilder.name(Topics.RIDE_UNMATCHED).partitions(Topics.PARTITIONS).build();
  }
}
