package io.dispatchgrid.common.redis;

import static org.assertj.core.api.Assertions.assertThat;

import io.dispatchgrid.common.geo.Geo;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RedisDriverIndexIT {

  @Container
  static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  static LettuceConnectionFactory factory;
  static StringRedisTemplate template;
  static RedisDriverIndex index;

  static final double LAT = 47.6062;
  static final double LNG = -122.3321;

  @BeforeAll
  static void connect() {
    factory = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
    factory.afterPropertiesSet();
    template = new StringRedisTemplate(factory);
    template.afterPropertiesSet();
    index = new RedisDriverIndex(template);
  }

  @AfterAll
  static void close() {
    factory.destroy();
  }

  @BeforeEach
  void flush() {
    template.getConnectionFactory().getConnection().serverCommands().flushAll();
  }

  private static DriverPosition at(String id, int city, double north, double east) {
    double[] p = Geo.offset(LAT, LNG, north, east);
    return new DriverPosition(id, city, p[0], p[1], DriverStatus.AVAILABLE, Instant.now());
  }

  @Test
  void nearbyReturnsNearestFirstWithinRadiusAndPerCity() {
    index.upsert(at("far", 1, 3000, 0), Duration.ofMinutes(1));
    index.upsert(at("near", 1, 100, 0), Duration.ofMinutes(1));
    index.upsert(at("mid", 1, 800, 0), Duration.ofMinutes(1));
    index.upsert(at("other-city", 2, 50, 0), Duration.ofMinutes(1));

    List<NearbyDriver> hits = index.nearby(1, LAT, LNG, 1000, 10);
    assertThat(hits).extracting(NearbyDriver::driverId).containsExactly("near", "mid");
    assertThat(hits.get(0).distanceMeters()).isBetween(90.0, 110.0);
    assertThat(index.nearby(1, LAT, LNG, 5000, 1)).extracting(NearbyDriver::driverId).containsExactly("near");
    assertThat(index.nearby(2, LAT, LNG, 1000, 10)).extracting(NearbyDriver::driverId).containsExactly("other-city");
    assertThat(index.size(1)).isEqualTo(3);
  }

  @Test
  void staleDriversAgeOutAndAreDroppedOnClaim() throws InterruptedException {
    index.upsert(at("d1", 1, 10, 0), Duration.ofMillis(500));
    assertThat(index.nearby(1, LAT, LNG, 1000, 10)).hasSize(1);
    Thread.sleep(800);
    assertThat(index.claim(1, "d1", "ride-x", Duration.ofSeconds(5))).isEqualTo(ClaimResult.STALE);
    assertThat(index.nearby(1, LAT, LNG, 1000, 10)).isEmpty();
    assertThat(index.size(1)).isZero();
  }

  @Test
  void offlineRemovesDriver() {
    index.upsert(at("d1", 1, 10, 0), Duration.ofMinutes(1));
    index.upsert(
        new DriverPosition("d1", 1, LAT, LNG, DriverStatus.OFFLINE, Instant.now()), Duration.ofMinutes(1));
    assertThat(index.size(1)).isZero();
  }

  @Test
  void claimIsExclusiveAndReleasable() {
    index.upsert(at("d1", 1, 10, 0), Duration.ofMinutes(1));
    assertThat(index.claim(1, "d1", "ride-a", Duration.ofSeconds(10))).isEqualTo(ClaimResult.CLAIMED);
    assertThat(index.claim(1, "d1", "ride-b", Duration.ofSeconds(10))).isEqualTo(ClaimResult.TAKEN);
    assertThat(index.claimedBy(1, "d1")).isEqualTo("ride-a");
    assertThat(index.nearby(1, LAT, LNG, 1000, 10)).isEmpty();
    index.upsert(at("d1", 1, 20, 0), Duration.ofMinutes(1));
    assertThat(index.nearby(1, LAT, LNG, 1000, 10)).as("pings do not resurface a claimed driver").isEmpty();
    assertThat(index.release(1, "d1", "ride-b")).isFalse();
    assertThat(index.release(1, "d1", "ride-a")).isTrue();
    assertThat(index.nearby(1, LAT, LNG, 1000, 10)).extracting(NearbyDriver::driverId).containsExactly("d1");
    assertThat(index.claim(1, "d1", "ride-b", Duration.ofSeconds(10))).isEqualTo(ClaimResult.CLAIMED);
  }

  @Test
  void claimExpiresAfterTtl() throws InterruptedException {
    index.upsert(at("d1", 1, 10, 0), Duration.ofMinutes(1));
    assertThat(index.claim(1, "d1", "ride-a", Duration.ofMillis(300))).isEqualTo(ClaimResult.CLAIMED);
    Thread.sleep(500);
    assertThat(index.claim(1, "d1", "ride-b", Duration.ofSeconds(10))).isEqualTo(ClaimResult.CLAIMED);
  }

  @Test
  void onlyOneOfManyConcurrentClaimsWins() throws Exception {
    index.upsert(at("hot", 1, 10, 0), Duration.ofMinutes(1));
    int contenders = 64;
    ExecutorService pool = Executors.newFixedThreadPool(16);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger wins = new AtomicInteger();
    List<Future<?>> futures = new java.util.ArrayList<>();
    for (int i = 0; i < contenders; i++) {
      String ride = "ride-" + i;
      futures.add(
          pool.submit(
              () -> {
                start.await();
                if (index.claim(1, "hot", ride, Duration.ofSeconds(30)) == ClaimResult.CLAIMED) {
                  wins.incrementAndGet();
                }
                return null;
              }));
    }
    start.countDown();
    for (Future<?> f : futures) {
      f.get();
    }
    pool.shutdown();
    assertThat(wins.get()).isEqualTo(1);
  }
}
