package io.dispatchgrid.common.health;

import io.dispatchgrid.common.shard.CityShardRouter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/** Readiness: every shard must answer a cheap validity check. */
public class ShardHealthIndicator implements HealthIndicator {
  private final CityShardRouter router;

  public ShardHealthIndicator(CityShardRouter router) {
    this.router = router;
  }

  @Override
  public Health health() {
    Map<String, Object> details = new LinkedHashMap<>();
    boolean allUp = true;
    for (int i = 0; i < router.shardCount(); i++) {
      try (Connection c = router.shard(i).getConnection()) {
        boolean ok = c.isValid(2);
        details.put("shard-" + i, ok ? "UP" : "DOWN");
        allUp &= ok;
      } catch (SQLException e) {
        details.put("shard-" + i, "DOWN: " + e.getMessage());
        allUp = false;
      }
    }
    return (allUp ? Health.up() : Health.down()).withDetails(details).build();
  }
}
