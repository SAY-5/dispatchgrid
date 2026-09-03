package io.dispatchgrid.common.health;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;

/** Readiness: the broker cluster must describe itself within a short deadline. */
public class KafkaHealthIndicator implements HealthIndicator {
  private final KafkaAdmin admin;

  public KafkaHealthIndicator(KafkaAdmin admin) {
    this.admin = admin;
  }

  @Override
  public Health health() {
    try (AdminClient client = AdminClient.create(admin.getConfigurationProperties())) {
      var cluster = client.describeCluster(new DescribeClusterOptions().timeoutMs(3000));
      int nodes = cluster.nodes().get(3, TimeUnit.SECONDS).size();
      return Health.up().withDetail("nodes", nodes).build();
    } catch (Exception e) {
      return Health.down(e).build();
    }
  }
}
