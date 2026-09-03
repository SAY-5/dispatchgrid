package io.dispatchgrid.matching;

import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.shard.TripRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Glue between the stream and the stores: run the matcher, persist the outcome in the city
 * shard, and update stats. Delivery is at-least-once, so a redelivered request whose trip is no
 * longer REQUESTED releases the claim it just took and is dropped instead of being emitted twice.
 */
public class MatchService {
  private static final Logger log = LoggerFactory.getLogger(MatchService.class);

  private final Matcher matcher;
  private final TripRepository trips;
  private final DriverIndex index;
  private final MatchStats stats;

  public MatchService(Matcher matcher, TripRepository trips, DriverIndex index, MatchStats stats) {
    this.matcher = matcher;
    this.trips = trips;
    this.index = index;
    this.stats = stats;
  }

  /** Returns zero or one outcome to emit downstream. */
  public List<MatchOutcome> handle(RideRequest request) {
    MatchOutcome outcome = matcher.match(request);
    if (outcome.isMatched()) {
      Match m = outcome.match();
      if (!trips.markMatched(m)) {
        index.release(m.cityId(), m.driverId(), m.rideId());
        stats.recordDropped();
        log.info("dropped duplicate request ride={} (trip no longer REQUESTED)", m.rideId());
        return List.of();
      }
      stats.recordMatch(m.matchLatencyMs());
      return List.of(outcome);
    }
    var u = outcome.unmatched();
    if (!trips.markUnmatched(u.rideId(), u.cityId(), u)) {
      stats.recordDropped();
      return List.of();
    }
    stats.recordUnmatched();
    log.info("unmatched ride={} city={} reason={}", u.rideId(), u.cityId(), u.reason());
    return List.of(outcome);
  }
}
