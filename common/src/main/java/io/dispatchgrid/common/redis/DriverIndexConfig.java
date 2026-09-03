package io.dispatchgrid.common.redis;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Exposes the Redis driver index wherever a Redis connection is configured. */
@AutoConfiguration(after = RedisAutoConfiguration.class)
@ConditionalOnBean(StringRedisTemplate.class)
public class DriverIndexConfig {

  @Bean
  @ConditionalOnMissingBean(DriverIndex.class)
  public DriverIndex driverIndex(StringRedisTemplate redis) {
    return new RedisDriverIndex(redis);
  }
}
