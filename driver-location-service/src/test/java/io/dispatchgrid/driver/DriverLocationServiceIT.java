package io.dispatchgrid.driver;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.dispatchgrid.common.geo.Geo;
import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.DriverPosition;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;

@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "driver-location.heartbeat-ttl=2s")
class DriverLocationServiceIT {
  static final double LAT = 47.6062;
  static final double LNG = -122.3321;

  @Container
  static GenericContainer<?> redis =
      new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  @Container
  static RedpandaContainer kafka = new RedpandaContainer("redpandadata/redpanda:v24.3.18");

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry r) {
    r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    r.add("spring.data.redis.host", redis::getHost);
    r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
  }

  @Autowired TestRestTemplate rest;

  @Test
  void positionsAreSearchableNearestFirstAndAgeOut() throws Exception {
    post("d-far", 2, 2500, 0);
    post("d-near", 2, 150, 0);
    post("d-mid", 2, 900, 0);
    post("d-elsewhere", 1, 100, 0);

    JsonNode hits =
        rest.getForObject(
            "/drivers/nearby?city=2&lat=" + LAT + "&lng=" + LNG + "&radius=2000&limit=10",
            JsonNode.class);
    assertThat(hits).extracting(n -> n.get("driverId").asText()).containsExactly("d-near", "d-mid");
    assertThat(rest.getForObject("/drivers/count?city=2", JsonNode.class).get("drivers").asLong())
        .isEqualTo(3);

    List<ConsumerRecord<String, byte[]>> records = consume(Topics.DRIVER_POSITIONS, 4);
    assertThat(records).extracting(ConsumerRecord::key).contains("1", "2");
    assertThat(records)
        .extracting(rec -> Json.read(rec.value(), DriverPosition.class).driverId())
        .contains("d-near", "d-elsewhere");

    Thread.sleep(2500);
    JsonNode after =
        rest.getForObject(
            "/drivers/nearby?city=2&lat=" + LAT + "&lng=" + LNG + "&radius=2000&limit=10",
            JsonNode.class);
    assertThat(after).as("geo entries linger until claimed, heartbeat hashes expire").hasSize(2);
  }

  @Test
  void readinessReportsRedisAndKafka() {
    ResponseEntity<JsonNode> res = rest.getForEntity("/actuator/health/readiness", JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody().get("status").asText()).isEqualTo("UP");
    assertThat(res.getBody().get("components").get("redis").get("status").asText()).isEqualTo("UP");
    assertThat(res.getBody().get("components").get("kafka").get("status").asText()).isEqualTo("UP");
  }

  private void post(String id, int city, double north, double east) {
    double[] p = Geo.offset(LAT, LNG, north, east);
    ResponseEntity<JsonNode> res =
        rest.postForEntity(
            "/drivers/" + id + "/position",
            Map.of("cityId", city, "lat", p[0], "lng", p[1]),
            JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
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
