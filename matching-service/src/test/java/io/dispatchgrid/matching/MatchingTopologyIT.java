package io.dispatchgrid.matching;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.dispatchgrid.common.geo.Geo;
import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.serde.Json;
import io.dispatchgrid.common.shard.CityShardRouter;
import io.dispatchgrid.common.shard.TripRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * Whole topology against real brokers and stores: N requests in, N matches out, no driver shared,
 * rows updated in the right shard, and throughput above the 500 per minute target.
 */
@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"matching.claim-ttl=10m", "matching.initial-radius-meters=1000"})
class MatchingTopologyIT {
  static final int RIDES = 300;
  static final int DRIVERS_PER_CITY = 200;
  static final Map<Integer, double[]> CENTERS =
      Map.of(1, new double[] {30.2672, -97.7431}, 2, new double[] {47.6062, -122.3321});

  @Container static MySQLContainer<?> shard0 = mysql();
  @Container static MySQLContainer<?> shard1 = mysql();

  /** Socket timeouts keep a stalled handshake from hanging the container startup wait. */
  static MySQLContainer<?> mysql() {
    return new MySQLContainer<>("mysql:8.0")
        .withUrlParam("connectTimeout", "5000")
        .withUrlParam("socketTimeout", "30000");
  }

  @Container
  static GenericContainer<?> redis =
      new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  @Container
  static RedpandaContainer kafka = new RedpandaContainer("redpandadata/redpanda:v24.3.18");

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) {
    r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    r.add(
        "spring.kafka.streams.properties.state.dir",
        () -> System.getProperty("java.io.tmpdir") + "/ks-" + UUID.randomUUID());
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    r.add("dispatchgrid.shards[0].url", shard0::getJdbcUrl);
    r.add("dispatchgrid.shards[0].username", shard0::getUsername);
    r.add("dispatchgrid.shards[0].password", shard0::getPassword);
    r.add("dispatchgrid.shards[1].url", shard1::getJdbcUrl);
    r.add("dispatchgrid.shards[1].username", shard1::getUsername);
    r.add("dispatchgrid.shards[1].password", shard1::getPassword);
  }

  @Autowired DriverIndex index;
  @Autowired TripRepository trips;
  @Autowired CityShardRouter router;
  @Autowired TestRestTemplate rest;

  @Test
  void matchesEveryRequestExactlyOnceAboveFiveHundredPerMinute() throws Exception {
    Random rnd = new Random(1);
    for (int city : CENTERS.keySet()) {
      for (int i = 0; i < DRIVERS_PER_CITY; i++) {
        index.upsert(spot("d-" + city + "-" + i, city, rnd), Duration.ofMinutes(10));
      }
    }
    waitForStreamsReady();

    List<RideRequest> requests = new ArrayList<>();
    for (int i = 0; i < RIDES; i++) {
      int city = (i % 2) + 1;
      double[] p = jitter(city, rnd, 2500);
      requests.add(
          new RideRequest(
              UUID.randomUUID().toString(),
              "rider-" + i,
              city,
              p[0],
              p[1],
              p[0],
              p[1],
              Instant.now()));
    }
    requests.forEach(trips::insertRequested);

    long started = System.nanoTime();
    try (KafkaProducer<String, byte[]> producer = producer()) {
      for (RideRequest r : requests) {
        producer.send(
            new ProducerRecord<>(Topics.RIDE_REQUESTS, Topics.cityKey(r.cityId()), Json.write(r)));
      }
      producer.flush();
    }

    List<Match> matches = consumeMatches(RIDES, Duration.ofSeconds(90));
    long elapsedMs = (System.nanoTime() - started) / 1_000_000;
    double perMinute = matches.size() * 60_000.0 / Math.max(elapsedMs, 1);
    System.out.printf(
        "matched %d rides in %d ms (%.0f per minute)%n", matches.size(), elapsedMs, perMinute);

    assertThat(matches).hasSize(RIDES);
    assertThat(matches).extracting(Match::rideId).doesNotHaveDuplicates();
    assertThat(matches).extracting(Match::driverId).doesNotHaveDuplicates();
    assertThat(matches)
        .allSatisfy(m -> assertThat(m.driverId()).startsWith("d-" + m.cityId() + "-"));
    assertThat(perMinute).isGreaterThanOrEqualTo(500);
    assertThat(matches).allSatisfy(m -> assertThat(m.surgeMultiplier()).isBetween(1.0, 3.0));

    JdbcTemplate s0 = new JdbcTemplate(router.shard(0));
    JdbcTemplate s1 = new JdbcTemplate(router.shard(1));
    assertThat(count(s1, "city_id = 1 AND status = 'MATCHED' AND driver_id IS NOT NULL"))
        .isEqualTo(RIDES / 2);
    assertThat(count(s0, "city_id = 2 AND status = 'MATCHED' AND driver_id IS NOT NULL"))
        .isEqualTo(RIDES / 2);
    assertThat(count(s0, "city_id = 1")).isZero();
    assertThat(count(s1, "city_id = 2")).isZero();
    assertThat(s1.queryForObject("SELECT COUNT(*) FROM drivers", Integer.class))
        .isEqualTo(RIDES / 2);
    assertThat(
            s1.queryForObject(
                "SELECT COUNT(*) FROM ride_events WHERE event_type = 'ride.matched'",
                Integer.class))
        .isEqualTo(RIDES / 2);

    JsonNode pricing = rest.getForObject("/pricing/1", JsonNode.class);
    assertThat(pricing.get("cityId").asInt()).isEqualTo(1);
    assertThat(pricing.get("cells").size()).isGreaterThan(0);
    assertThat(
            s1.queryForObject(
                "SELECT MIN(surge_multiplier) FROM trips WHERE status = 'MATCHED'", Double.class))
        .isGreaterThanOrEqualTo(1.0);

    JsonNode stats = rest.getForObject("/matching/stats", JsonNode.class);
    assertThat(stats.get("matched").asLong()).isEqualTo(RIDES);
    assertThat(stats.get("unmatched").asLong()).isZero();
    assertThat(stats.get("p95LatencyMs").asLong())
        .isGreaterThanOrEqualTo(stats.get("p50LatencyMs").asLong());

    // at-least-once redelivery: the same request again must not produce a second match
    try (KafkaProducer<String, byte[]> producer = producer()) {
      RideRequest again = requests.get(0);
      producer.send(
          new ProducerRecord<>(
              Topics.RIDE_REQUESTS, Topics.cityKey(again.cityId()), Json.write(again)));
      producer.flush();
    }
    long deadline = System.currentTimeMillis() + 20_000;
    while (rest.getForObject("/matching/stats", JsonNode.class).get("dropped").asLong() < 1
        && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
    }
    JsonNode after = rest.getForObject("/matching/stats", JsonNode.class);
    assertThat(after.get("dropped").asLong()).isEqualTo(1);
    assertThat(after.get("matched").asLong()).isEqualTo(RIDES);
    assertThat(index.size(1) + index.size(2)).isEqualTo(2L * DRIVERS_PER_CITY - RIDES);

    JsonNode readiness = rest.getForObject("/actuator/health/readiness", JsonNode.class);
    assertThat(readiness.get("status").asText()).isEqualTo("UP");
    assertThat(readiness.get("components").get("streams").get("details").get("state").asText())
        .isEqualTo("RUNNING");
  }

  private void waitForStreamsReady() throws InterruptedException {
    long deadline = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < deadline) {
      JsonNode h = rest.getForObject("/actuator/health/readiness", JsonNode.class);
      if (h != null && "UP".equals(h.get("status").asText())) {
        return;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("streams never became ready");
  }

  private static int count(JdbcTemplate jdbc, String where) {
    return jdbc.queryForObject("SELECT COUNT(*) FROM trips WHERE " + where, Integer.class);
  }

  private static double[] jitter(int city, Random rnd, double radius) {
    double[] c = CENTERS.get(city);
    double r = radius * Math.sqrt(rnd.nextDouble());
    double a = rnd.nextDouble() * 2 * Math.PI;
    return Geo.offset(c[0], c[1], r * Math.cos(a), r * Math.sin(a));
  }

  private static DriverPosition spot(String id, int city, Random rnd) {
    double[] p = jitter(city, rnd, 3000);
    return new DriverPosition(id, city, p[0], p[1], DriverStatus.AVAILABLE, Instant.now());
  }

  private static KafkaProducer<String, byte[]> producer() {
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return new KafkaProducer<>(p, new StringSerializer(), new ByteArraySerializer());
  }

  private static List<Match> consumeMatches(int expected, Duration timeout) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<Match> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(p, new StringDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(List.of(Topics.RIDE_MATCHES));
      long deadline = System.currentTimeMillis() + timeout.toMillis();
      while (out.size() < expected && System.currentTimeMillis() < deadline) {
        consumer
            .poll(Duration.ofMillis(300))
            .forEach(rec -> out.add(Json.read(rec.value(), Match.class)));
      }
    }
    return out;
  }
}
