package io.dispatchgrid.common.shard;

import java.util.Set;
import org.springframework.boot.autoconfigure.AutoConfigurationImportFilter;
import org.springframework.boot.autoconfigure.AutoConfigurationMetadata;

/**
 * There is no primary datasource in this system, only shards, so Spring Boot's single-datasource
 * and Flyway auto-configuration are switched off for every service that depends on this module.
 */
public class SingleDataSourceExclusionFilter implements AutoConfigurationImportFilter {
  private static final Set<String> EXCLUDED =
      Set.of(
          "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
          "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
          "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
          "org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration");

  @Override
  public boolean[] match(String[] classNames, AutoConfigurationMetadata metadata) {
    boolean[] out = new boolean[classNames.length];
    for (int i = 0; i < classNames.length; i++) {
      out[i] = classNames[i] == null || !EXCLUDED.contains(classNames[i]);
    }
    return out;
  }
}
