package io.dispatchgrid.rider;

import io.dispatchgrid.common.serde.Json;
import io.dispatchgrid.common.shard.CityShardRouter;
import io.dispatchgrid.common.shard.TripRepository;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class RiderConfig {

  @Bean
  public TripRepository tripRepository(CityShardRouter router) {
    return new TripRepository(router);
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
