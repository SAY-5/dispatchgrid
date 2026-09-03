package io.dispatchgrid.common.health;

import io.dispatchgrid.common.shard.CityShardRouter;
import io.dispatchgrid.common.shard.ShardDataSourcesConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Registers the readiness contributors. Liveness stays a plain JVM check so a broker outage pulls
 * the pod out of the Service without restarting it.
 */
@AutoConfiguration(after = {KafkaAutoConfiguration.class, ShardDataSourcesConfig.class})
public class HealthConfig {

  @Bean
  @ConditionalOnBean(CityShardRouter.class)
  public ShardHealthIndicator shardsHealthIndicator(CityShardRouter router) {
    return new ShardHealthIndicator(router);
  }

  @Bean
  @ConditionalOnBean(KafkaAdmin.class)
  public KafkaHealthIndicator kafkaHealthIndicator(KafkaAdmin admin) {
    return new KafkaHealthIndicator(admin);
  }
}
