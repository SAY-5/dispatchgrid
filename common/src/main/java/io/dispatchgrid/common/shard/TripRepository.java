package io.dispatchgrid.common.shard;

import com.fasterxml.jackson.databind.JsonNode;
import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.model.TripEventType;
import io.dispatchgrid.common.model.TripStatus;
import io.dispatchgrid.common.serde.Json;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/** Trip persistence. Every method resolves the shard from the city id first. */
public class TripRepository {
  private final CityShardRouter router;

  public TripRepository(CityShardRouter router) {
    this.router = router;
  }

  public record Trip(
      String rideId,
      String riderId,
      int cityId,
      TripStatus status,
      double pickupLat,
      double pickupLng,
      double dropoffLat,
      double dropoffLng,
      String driverId,
      Integer matchLatencyMs,
      Integer searchRadiusMeters,
      Integer driverDistanceMeters,
      Double surgeMultiplier,
      Instant requestedAt,
      Instant matchedAt,
      Instant completedAt,
      Instant cancelledAt,
      int shard) {}

  /** One row of the trip's timeline, oldest first. */
  public record Event(long id, String type, JsonNode payload, Instant createdAt) {}

  private JdbcTemplate jdbc(int cityId) {
    return new JdbcTemplate(router.dataSourceFor(cityId));
  }

  public void insertRequested(RideRequest r) {
    JdbcTemplate jdbc = jdbc(r.cityId());
    jdbc.update(
        """
        INSERT INTO trips (ride_id, rider_id, city_id, status, pickup_lat, pickup_lng,
                           dropoff_lat, dropoff_lng, requested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        r.rideId(),
        r.riderId(),
        r.cityId(),
        TripStatus.REQUESTED.name(),
        r.pickupLat(),
        r.pickupLng(),
        r.dropoffLat(),
        r.dropoffLng(),
        Timestamp.from(r.requestedAt()));
    insertEvent(jdbc, r.rideId(), r.cityId(), "ride.requested", r);
  }

  /** Marks the trip matched. Returns false if the row was not in REQUESTED state. */
  public boolean markMatched(Match m) {
    JdbcTemplate jdbc = jdbc(m.cityId());
    int updated =
        jdbc.update(
            """
            UPDATE trips SET status = ?, driver_id = ?, match_latency_ms = ?, search_radius_m = ?,
                             driver_distance_m = ?, surge_multiplier = ?, matched_at = ?
            WHERE ride_id = ? AND status = ?
            """,
            TripStatus.MATCHED.name(),
            m.driverId(),
            (int) m.matchLatencyMs(),
            m.searchRadiusMeters(),
            (int) Math.round(m.driverDistanceMeters()),
            m.surgeMultiplier(),
            Timestamp.from(m.matchedAt()),
            m.rideId(),
            TripStatus.REQUESTED.name());
    if (updated == 0) {
      return false;
    }
    jdbc.update(
        """
        INSERT INTO drivers (driver_id, city_id, total_matches, last_matched_at, last_ride_id)
        VALUES (?, ?, 1, ?, ?)
        ON DUPLICATE KEY UPDATE total_matches = total_matches + 1,
                                last_matched_at = VALUES(last_matched_at),
                                last_ride_id = VALUES(last_ride_id)
        """,
        m.driverId(),
        m.cityId(),
        Timestamp.from(m.matchedAt()),
        m.rideId());
    insertEvent(jdbc, m.rideId(), m.cityId(), "ride.matched", m);
    return true;
  }

  public boolean markUnmatched(String rideId, int cityId, Object payload) {
    JdbcTemplate jdbc = jdbc(cityId);
    int updated =
        jdbc.update(
            "UPDATE trips SET status = ? WHERE ride_id = ? AND status = ?",
            TripStatus.UNMATCHED.name(),
            rideId,
            TripStatus.REQUESTED.name());
    if (updated == 0) {
      return false;
    }
    insertEvent(jdbc, rideId, cityId, "ride.unmatched", payload);
    return true;
  }

  /**
   * Cancels a trip that is still REQUESTED or MATCHED. Returns the row as it was before the
   * transition so the caller knows whether a driver has to be released, or empty when the trip was
   * unknown or already final.
   */
  public Optional<Trip> markCancelled(String rideId, int cityId, Instant at) {
    Optional<Trip> before = find(cityId, rideId);
    if (before.isEmpty()) {
      return Optional.empty();
    }
    Trip t = before.get();
    if (t.status() != TripStatus.REQUESTED && t.status() != TripStatus.MATCHED) {
      return Optional.empty();
    }
    JdbcTemplate jdbc = jdbc(cityId);
    int updated =
        jdbc.update(
            "UPDATE trips SET status = ?, cancelled_at = ? WHERE ride_id = ? AND status = ?",
            TripStatus.CANCELLED.name(),
            Timestamp.from(at),
            rideId,
            t.status().name());
    if (updated == 0) {
      return Optional.empty();
    }
    insertEvent(
        jdbc,
        rideId,
        cityId,
        "ride.cancelled",
        new TripEvent(rideId, cityId, t.driverId(), TripEventType.CANCELLED, at));
    return before;
  }

  /** Completes a MATCHED trip; only the driver it was matched to can complete it. */
  public boolean markCompleted(String rideId, int cityId, String driverId, Instant at) {
    JdbcTemplate jdbc = jdbc(cityId);
    int updated =
        jdbc.update(
            """
            UPDATE trips SET status = ?, completed_at = ?
            WHERE ride_id = ? AND status = ? AND driver_id = ?
            """,
            TripStatus.COMPLETED.name(),
            Timestamp.from(at),
            rideId,
            TripStatus.MATCHED.name(),
            driverId);
    if (updated == 0) {
      return false;
    }
    insertEvent(
        jdbc,
        rideId,
        cityId,
        "ride.completed",
        new TripEvent(rideId, cityId, driverId, TripEventType.COMPLETED, at));
    return true;
  }

  public Optional<Trip> find(int cityId, String rideId) {
    int shard = router.shardIndexFor(cityId);
    List<Trip> rows =
        jdbc(cityId).query("SELECT * FROM trips WHERE ride_id = ?", mapper(shard), rideId);
    return rows.stream().findFirst();
  }

  /** The trip's timeline from ride_events, oldest first; empty for an unknown ride. */
  public List<Event> events(int cityId, String rideId) {
    return jdbc(cityId)
        .query(
            "SELECT id, event_type, payload, created_at FROM ride_events WHERE ride_id = ?"
                + " ORDER BY id",
            (rs, i) -> {
              String raw = rs.getString("payload");
              JsonNode payload = raw == null ? null : Json.read(raw.getBytes(), JsonNode.class);
              return new Event(
                  rs.getLong("id"),
                  rs.getString("event_type"),
                  payload,
                  rs.getTimestamp("created_at").toInstant());
            },
            rideId);
  }

  /** Looks in every shard; used when the caller does not know the city. */
  public Optional<Trip> findAnywhere(String rideId) {
    for (int i = 0; i < router.shardCount(); i++) {
      List<Trip> rows =
          new JdbcTemplate(router.shard(i))
              .query("SELECT * FROM trips WHERE ride_id = ?", mapper(i), rideId);
      if (!rows.isEmpty()) {
        return Optional.of(rows.get(0));
      }
    }
    return Optional.empty();
  }

  /** Per-shard counts keyed by "shard-i" then "city:status". */
  public Map<String, Map<String, Long>> countsByShard() {
    Map<String, Map<String, Long>> out = new LinkedHashMap<>();
    for (int i = 0; i < router.shardCount(); i++) {
      Map<String, Long> counts = new LinkedHashMap<>();
      new JdbcTemplate(router.shard(i))
          .query(
              "SELECT city_id, status, COUNT(*) AS n FROM trips GROUP BY city_id, status"
                  + " ORDER BY city_id, status",
              rs -> {
                counts.put(rs.getInt("city_id") + ":" + rs.getString("status"), rs.getLong("n"));
              });
      out.put("shard-" + i, counts);
    }
    return out;
  }

  /** Appends a timeline entry without touching the trip row (retries, offers, notes). */
  public void appendEvent(String rideId, int cityId, String type, Object payload) {
    insertEvent(jdbc(cityId), rideId, cityId, type, payload);
  }

  private static void insertEvent(
      JdbcTemplate jdbc, String rideId, int cityId, String type, Object payload) {
    jdbc.update(
        "INSERT INTO ride_events (ride_id, city_id, event_type, payload) VALUES (?, ?, ?, ?)",
        rideId,
        cityId,
        type,
        new String(Json.write(payload)));
  }

  private static RowMapper<Trip> mapper(int shard) {
    return (rs, i) -> {
      Timestamp matched = rs.getTimestamp("matched_at");
      Timestamp completed = rs.getTimestamp("completed_at");
      Timestamp cancelled = rs.getTimestamp("cancelled_at");
      Integer latency = rs.getObject("match_latency_ms", Integer.class);
      Integer radius = rs.getObject("search_radius_m", Integer.class);
      Integer distance = rs.getObject("driver_distance_m", Integer.class);
      Double surge = rs.getObject("surge_multiplier", Double.class);
      return new Trip(
          rs.getString("ride_id"),
          rs.getString("rider_id"),
          rs.getInt("city_id"),
          TripStatus.valueOf(rs.getString("status")),
          rs.getDouble("pickup_lat"),
          rs.getDouble("pickup_lng"),
          rs.getDouble("dropoff_lat"),
          rs.getDouble("dropoff_lng"),
          rs.getString("driver_id"),
          latency,
          radius,
          distance,
          surge,
          rs.getTimestamp("requested_at").toInstant(),
          matched == null ? null : matched.toInstant(),
          completed == null ? null : completed.toInstant(),
          cancelled == null ? null : cancelled.toInstant(),
          shard);
    };
  }
}
