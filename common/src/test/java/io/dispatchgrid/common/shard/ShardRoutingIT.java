package io.dispatchgrid.common.shard;

import static org.assertj.core.api.Assertions.assertThat;

import io.dispatchgrid.common.TestApp;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.TripStatus;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(
    classes = TestApp.class,
    properties =
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration")
class ShardRoutingIT {

  @Container static MySQLContainer<?> shard0 = new MySQLContainer<>("mysql:8.0");
  @Container static MySQLContainer<?> shard1 = new MySQLContainer<>("mysql:8.0");

  @DynamicPropertySource
  static void shards(DynamicPropertyRegistry r) {
    r.add("dispatchgrid.shards[0].name", () -> "shard-0");
    r.add("dispatchgrid.shards[0].url", shard0::getJdbcUrl);
    r.add("dispatchgrid.shards[0].username", shard0::getUsername);
    r.add("dispatchgrid.shards[0].password", shard0::getPassword);
    r.add("dispatchgrid.shards[1].name", () -> "shard-1");
    r.add("dispatchgrid.shards[1].url", shard1::getJdbcUrl);
    r.add("dispatchgrid.shards[1].username", shard1::getUsername);
    r.add("dispatchgrid.shards[1].password", shard1::getPassword);
  }

  @Autowired CityShardRouter router;

  @Test
  void migrationsApplyToEveryShard() {
    for (int i = 0; i < router.shardCount(); i++) {
      JdbcTemplate jdbc = new JdbcTemplate(router.shard(i));
      Integer applied =
          jdbc.queryForObject(
              "SELECT COUNT(*) FROM flyway_schema_history WHERE success = 1", Integer.class);
      assertThat(applied).isGreaterThanOrEqualTo(1);
      assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM trips", Integer.class)).isZero();
      assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM drivers", Integer.class)).isZero();
      assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM ride_events", Integer.class)).isZero();
    }
  }

  @Test
  void tripLandsInTheShardForItsCity() {
    TripRepository trips = new TripRepository(router);
    RideRequest city1 = request(1);
    RideRequest city2 = request(2);
    trips.insertRequested(city1);
    trips.insertRequested(city2);

    JdbcTemplate s0 = new JdbcTemplate(router.shard(0));
    JdbcTemplate s1 = new JdbcTemplate(router.shard(1));
    assertThat(count(s1, city1.rideId())).isEqualTo(1);
    assertThat(count(s0, city1.rideId())).isZero();
    assertThat(count(s0, city2.rideId())).isEqualTo(1);
    assertThat(count(s1, city2.rideId())).isZero();

    var found = trips.find(1, city1.rideId()).orElseThrow();
    assertThat(found.shard()).isEqualTo(1);
    assertThat(found.status()).isEqualTo(TripStatus.REQUESTED);
    assertThat(trips.findAnywhere(city2.rideId()).orElseThrow().shard()).isEqualTo(0);

    var counts = trips.countsByShard();
    assertThat(counts.get("shard-1")).containsEntry("1:REQUESTED", 1L);
    assertThat(counts.get("shard-0")).containsEntry("2:REQUESTED", 1L);
    assertThat(s1.queryForObject("SELECT COUNT(*) FROM ride_events WHERE ride_id = ?", Integer.class, city1.rideId()))
        .isEqualTo(1);
  }

  private static int count(JdbcTemplate jdbc, String rideId) {
    return jdbc.queryForObject("SELECT COUNT(*) FROM trips WHERE ride_id = ?", Integer.class, rideId);
  }

  private static RideRequest request(int cityId) {
    return new RideRequest(
        UUID.randomUUID().toString(), "rider-" + cityId, cityId, 47.60, -122.33, 47.61, -122.34, Instant.now());
  }
}
