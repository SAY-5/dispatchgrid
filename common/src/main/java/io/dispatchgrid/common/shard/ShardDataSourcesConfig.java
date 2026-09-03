package io.dispatchgrid.common.shard;

import com.zaxxer.hikari.HikariDataSource;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Builds one Hikari pool per configured shard, applies Flyway migrations to each, and exposes the
 * router. Spring Boot's single-DataSource and Flyway auto-configuration are excluded because
 * there is intentionally no primary datasource.
 */
@AutoConfiguration(before = {DataSourceAutoConfiguration.class, FlywayAutoConfiguration.class})
@EnableConfigurationProperties(ShardProperties.class)
@ConditionalOnProperty(prefix = "dispatchgrid", name = "shards[0].url")
public class ShardDataSourcesConfig {
  private static final Logger log = LoggerFactory.getLogger(ShardDataSourcesConfig.class);

  @Bean(destroyMethod = "close")
  public ShardPools shardPools(ShardProperties props) {
    List<HikariDataSource> pools = new ArrayList<>();
    for (int i = 0; i < props.getShards().size(); i++) {
      ShardProperties.Shard s = props.getShards().get(i);
      HikariDataSource ds = new HikariDataSource();
      ds.setPoolName(s.getName() != null ? s.getName() : "shard-" + i);
      ds.setJdbcUrl(s.getUrl());
      ds.setUsername(s.getUsername());
      ds.setPassword(s.getPassword());
      ds.setMaximumPoolSize(s.getMaxPoolSize());
      ds.setConnectionTimeout(10_000);
      pools.add(ds);
    }
    if (props.isMigrate()) {
      for (HikariDataSource ds : pools) {
        log.info("applying migrations to {}", ds.getPoolName());
        Flyway.configure()
            .dataSource(ds)
            .locations("classpath:db/migration")
            .connectRetries(30)
            .load()
            .migrate();
      }
    }
    return new ShardPools(pools);
  }

  @Bean
  public CityShardRouter cityShardRouter(ShardPools pools, ShardProperties props) {
    return new CityShardRouter(new ArrayList<DataSource>(pools.pools()), props.getOverrides());
  }

  /** Owns the pools so they are closed together on shutdown. */
  public record ShardPools(List<HikariDataSource> pools) implements AutoCloseable {
    @Override
    public void close() {
      pools.forEach(HikariDataSource::close);
    }
  }
}
