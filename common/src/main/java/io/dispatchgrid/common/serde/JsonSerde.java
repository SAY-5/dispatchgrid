package io.dispatchgrid.common.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Kafka Serde backed by the shared Jackson mapper. Null values pass through as null. */
public final class JsonSerde<T> implements Serde<T> {
  private final Class<T> type;

  public JsonSerde(Class<T> type) {
    this.type = type;
  }

  public static <T> JsonSerde<T> of(Class<T> type) {
    return new JsonSerde<>(type);
  }

  @Override
  public Serializer<T> serializer() {
    return (topic, data) -> data == null ? null : Json.write(data);
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, bytes) -> bytes == null ? null : Json.read(bytes, type);
  }
}
