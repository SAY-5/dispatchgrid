package io.dispatchgrid.rider;

import io.dispatchgrid.common.kafka.Topics;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.model.TripEventType;
import io.dispatchgrid.common.shard.CityShardRouter;
import io.dispatchgrid.common.shard.TripRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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
@RequestMapping("/rides")
public class RideController {
  private final TripRepository trips;
  private final CityShardRouter router;
  private final KafkaTemplate<String, Object> kafka;
  private final Clock clock;

  @Autowired
  public RideController(
      TripRepository trips, CityShardRouter router, KafkaTemplate<String, Object> kafka) {
    this(trips, router, kafka, Clock.systemUTC());
  }

  RideController(
      TripRepository trips,
      CityShardRouter router,
      KafkaTemplate<String, Object> kafka,
      Clock clock) {
    this.trips = trips;
    this.router = router;
    this.kafka = kafka;
    this.clock = clock;
  }

  public record CreateRide(
      @NotBlank String riderId,
      @NotNull @Min(0) Integer cityId,
      @NotNull @DecimalMin("-90") @DecimalMax("90") Double pickupLat,
      @NotNull @DecimalMin("-180") @DecimalMax("180") Double pickupLng,
      @NotNull @DecimalMin("-90") @DecimalMax("90") Double dropoffLat,
      @NotNull @DecimalMin("-180") @DecimalMax("180") Double dropoffLng) {}

  public record RideCreated(
      String rideId, int cityId, int shard, String status, Instant requestedAt) {}

  @PostMapping
  public ResponseEntity<RideCreated> create(@Valid @RequestBody CreateRide body) {
    RideRequest request =
        new RideRequest(
            UUID.randomUUID().toString(),
            body.riderId(),
            body.cityId(),
            body.pickupLat(),
            body.pickupLng(),
            body.dropoffLat(),
            body.dropoffLng(),
            Instant.now(clock));
    trips.insertRequested(request);
    kafka.send(Topics.RIDE_REQUESTS, Topics.cityKey(request.cityId()), request);
    return ResponseEntity.status(HttpStatus.ACCEPTED)
        .body(
            new RideCreated(
                request.rideId(),
                request.cityId(),
                router.shardIndexFor(request.cityId()),
                "REQUESTED",
                request.requestedAt()));
  }

  @GetMapping("/{rideId}")
  public ResponseEntity<TripRepository.Trip> get(
      @PathVariable String rideId, @RequestParam(required = false) Integer city) {
    Optional<TripRepository.Trip> trip =
        city != null ? trips.find(city, rideId) : trips.findAnywhere(rideId);
    return trip.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
  }

  /**
   * Cancels a ride that is still REQUESTED or MATCHED. The row moves to CANCELLED here; the
   * matching service releases the driver's claim when it sees the lifecycle event. A ride that
   * already finished answers 409 with its current status.
   */
  @PostMapping("/{rideId}/cancel")
  public ResponseEntity<Map<String, Object>> cancel(
      @PathVariable String rideId, @RequestParam(required = false) Integer city) {
    Optional<TripRepository.Trip> trip =
        city != null ? trips.find(city, rideId) : trips.findAnywhere(rideId);
    if (trip.isEmpty()) {
      return ResponseEntity.notFound().build();
    }
    TripRepository.Trip t = trip.get();
    Instant now = Instant.now(clock);
    Optional<TripRepository.Trip> before = trips.markCancelled(rideId, t.cityId(), now);
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("rideId", rideId);
    body.put("cityId", t.cityId());
    if (before.isEmpty()) {
      body.put("status", t.status().name());
      return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }
    TripEvent event =
        new TripEvent(rideId, t.cityId(), before.get().driverId(), TripEventType.CANCELLED, now);
    kafka.send(Topics.RIDE_LIFECYCLE, Topics.cityKey(t.cityId()), event);
    body.put("status", "CANCELLED");
    body.put("previousStatus", before.get().status().name());
    body.put("driverId", before.get().driverId());
    body.put("cancelledAt", now);
    return ResponseEntity.ok(body);
  }

  /** Trip counts per shard, grouped by city and status. Shows where each city's rows live. */
  @GetMapping("/stats")
  public Map<String, Map<String, Long>> stats() {
    return trips.countsByShard();
  }
}
