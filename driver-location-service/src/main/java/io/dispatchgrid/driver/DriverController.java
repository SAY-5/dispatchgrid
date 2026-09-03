package io.dispatchgrid.driver;

import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.redis.NearbyDriver;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/drivers")
public class DriverController {
  private final DriverIndex index;
  private final KafkaTemplate<String, Object> kafka;
  private final DriverConfig.Props props;
  private final Clock clock;

  public DriverController(
      DriverIndex index, KafkaTemplate<String, Object> kafka, DriverConfig.Props props) {
    this(index, kafka, props, Clock.systemUTC());
  }

  DriverController(
      DriverIndex index,
      KafkaTemplate<String, Object> kafka,
      DriverConfig.Props props,
      Clock clock) {
    this.index = index;
    this.kafka = kafka;
    this.props = props;
    this.clock = clock;
  }

  public record PositionUpdate(
      @NotNull @Min(0) Integer cityId,
      @NotNull @DecimalMin("-90") @DecimalMax("90") Double lat,
      @NotNull @DecimalMin("-180") @DecimalMax("180") Double lng,
      DriverStatus status) {}

  @PostMapping("/{driverId}/position")
  public ResponseEntity<Map<String, Object>> position(
      @PathVariable String driverId, @Valid @RequestBody PositionUpdate body) {
    DriverStatus status = body.status() == null ? DriverStatus.AVAILABLE : body.status();
    DriverPosition p =
        new DriverPosition(
            driverId, body.cityId(), body.lat(), body.lng(), status, Instant.now(clock));
    index.upsert(p, props.heartbeatTtl());
    kafka.send(Topics.DRIVER_POSITIONS, Topics.cityKey(p.cityId()), p);
    return ResponseEntity.accepted()
        .body(
            Map.of(
                "driverId", driverId,
                "cityId", p.cityId(),
                "status", status.name(),
                "expiresInMs", props.heartbeatTtl().toMillis()));
  }

  @GetMapping("/nearby")
  public List<NearbyDriver> nearby(
      @RequestParam int city,
      @RequestParam double lat,
      @RequestParam double lng,
      @RequestParam(defaultValue = "2000") int radius,
      @RequestParam(defaultValue = "10") int limit) {
    return index.nearby(city, lat, lng, radius, Math.min(limit, props.maxNearby()));
  }

  @GetMapping("/count")
  public Map<String, Long> count(@RequestParam int city) {
    return Map.of("cityId", (long) city, "drivers", index.size(city));
  }
}
