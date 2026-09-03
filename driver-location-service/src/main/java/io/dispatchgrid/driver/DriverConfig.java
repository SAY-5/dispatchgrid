package io.dispatchgrid.driver;

import io.dispatchgrid.common.serde.Json;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class DriverConfig {

  /** Heartbeat TTL: a driver that stops pinging for this long disappears from matching. */
  @ConfigurationProperties(prefix = "driver-location")
  public record Props(Duration heartbeatTtl, int maxNearby) {
    public Props {
      if (heartbeatTtl == null) {
        heartbeatTtl = Duration.ofSeconds(15);
      }
      if (maxNearby <= 0) {
        maxNearby = 50;
      }
    }
  }

  @Bean
  public Props driverLocationProps() {
    return new Props(null, 0);
  }

  @Bean
  public ProducerFactory<String, Object> producerFactory(KafkaProperties props) {
    Map<String, Object> cfg = props.buildProducerProperties();
    Serializer<Object> value = (topic, data) -> data == null ? null : Json.write(data);
    return new DefaultKafkaProducerFactory<>(cfg, new StringSerializer(), value);
  }

  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> factory) {
    return new KafkaTemplate<>(factory);
  }
}
