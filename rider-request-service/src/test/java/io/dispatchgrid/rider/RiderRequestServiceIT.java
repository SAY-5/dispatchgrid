package io.dispatchgrid.rider;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.serde.Json;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RiderRequestServiceIT {

  @Container static MySQLContainer<?> shard0 = new MySQLContainer<>("mysql:8.0");
  @Container static MySQLContainer<?> shard1 = new MySQLContainer<>("mysql:8.0");

  @Container
  static RedpandaContainer kafka = new RedpandaContainer("redpandadata/redpanda:v24.3.18");

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) {
    r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    r.add("dispatchgrid.shards[0].url", shard0::getJdbcUrl);
    r.add("dispatchgrid.shards[0].username", shard0::getUsername);
    r.add("dispatchgrid.shards[0].password", shard0::getPassword);
    r.add("dispatchgrid.shards[1].url", shard1::getJdbcUrl);
    r.add("dispatchgrid.shards[1].username", shard1::getUsername);
    r.add("dispatchgrid.shards[1].password", shard1::getPassword);
  }

  @Autowired TestRestTemplate rest;

  @Test
  void requestLandsInCityShardAndOnTheTopicKeyedByCity() {
    String ride1 = create(1);
    String ride2 = create(2);

    JdbcTemplate s0 = new JdbcTemplate(ds(shard0));
    JdbcTemplate s1 = new JdbcTemplate(ds(shard1));
    assertThat(s1.queryForObject("SELECT status FROM trips WHERE ride_id = ?", String.class, ride1))
        .isEqualTo("REQUESTED");
    assertThat(s0.queryForObject("SELECT COUNT(*) FROM trips WHERE ride_id = ?", Integer.class, ride1))
        .isZero();
    assertThat(s0.queryForObject("SELECT city_id FROM trips WHERE ride_id = ?", Integer.class, ride2))
        .isEqualTo(2);

    List<ConsumerRecord<String, byte[]>> records = consume(Topics.RIDE_REQUESTS, 2);
    Map<String, String> keyByRide = new java.util.HashMap<>();
    for (ConsumerRecord<String, byte[]> rec : records) {
      RideRequest req = Json.read(rec.value(), RideRequest.class);
      keyByRide.put(req.rideId(), rec.key());
    }
    assertThat(keyByRide).containsEntry(ride1, "1").containsEntry(ride2, "2");

    ResponseEntity<JsonNode> get = rest.getForEntity("/rides/" + ride1, JsonNode.class);
    assertThat(get.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(get.getBody().get("shard").asInt()).isEqualTo(1);
    assertThat(rest.getForEntity("/rides/" + UUID.randomUUID(), JsonNode.class).getStatusCode())
        .isEqualTo(HttpStatus.NOT_FOUND);

    JsonNode stats = rest.getForObject("/rides/stats", JsonNode.class);
    assertThat(stats.get("shard-1").get("1:REQUESTED").asLong()).isEqualTo(1);
    assertThat(stats.get("shard-0").get("2:REQUESTED").asLong()).isEqualTo(1);
  }

  @Test
  void readinessReportsKafkaAndShards() {
    ResponseEntity<JsonNode> res = rest.getForEntity("/actuator/health/readiness", JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody().get("status").asText()).isEqualTo("UP");
    assertThat(res.getBody().get("components").get("kafka").get("status").asText()).isEqualTo("UP");
    assertThat(res.getBody().get("components").get("shards").get("details").get("shard-1").asText())
        .isEqualTo("UP");
    assertThat(rest.getForEntity("/actuator/health/liveness", JsonNode.class).getStatusCode())
        .isEqualTo(HttpStatus.OK);
  }

  private String create(int city) {
    Map<String, Object> body =
        Map.of(
            "riderId", "rider-" + city,
            "cityId", city,
            "pickupLat", 30.27,
            "pickupLng", -97.74,
            "dropoffLat", 30.28,
            "dropoffLng", -97.75);
    ResponseEntity<JsonNode> res = rest.postForEntity("/rides", body, JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    assertThat(res.getBody().get("shard").asInt()).isEqualTo(city % 2);
    return res.getBody().get("rideId").asText();
  }

  private static javax.sql.DataSource ds(MySQLContainer<?> c) {
    return new org.springframework.jdbc.datasource.DriverManagerDataSource(
        c.getJdbcUrl(), c.getUsername(), c.getPassword());
  }

  static List<ConsumerRecord<String, byte[]>> consume(String topic, int expected) {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> consumer =
        new KafkaConsumer<>(p, new StringDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(List.of(topic));
      long deadline = System.currentTimeMillis() + 30_000;
      while (out.size() < expected && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(500)).forEach(out::add);
      }
    }
    return out;
  }
}
