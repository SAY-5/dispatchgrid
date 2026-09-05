package io.dispatchgrid.matching;

import io.dispatchgrid.common.model.TripEvent;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.shard.TripRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies cancellations and completions to the driver side of a trip. A completion moves the row
 * (only the matched driver may complete it) and then frees the claim; a cancellation was already
 * applied to the row by the rider service, so only the claim is released here. Both are idempotent
 * under redelivery because a release of a claim the ride no longer holds is a no-op.
 */
public class LifecycleService {
  private static final Logger log = LoggerFactory.getLogger(LifecycleService.class);

  private final TripRepository trips;
  private final DriverIndex index;
  private final MatchStats stats;

  public LifecycleService(TripRepository trips, DriverIndex index, MatchStats stats) {
    this.trips = trips;
    this.index = index;
    this.stats = stats;
  }

  public void handle(TripEvent e) {
    switch (e.type()) {
      case COMPLETED -> {
        if (!trips.markCompleted(e.rideId(), e.cityId(), e.driverId(), e.at())) {
          stats.recordLifecycleIgnored();
          log.info(
              "ignored completion ride={} driver={} (not MATCHED to that driver)",
              e.rideId(),
              e.driverId());
          return;
        }
        index.release(e.cityId(), e.driverId(), e.rideId());
        stats.recordCompleted();
      }
      case CANCELLED -> {
        if (e.driverId() != null) {
          index.release(e.cityId(), e.driverId(), e.rideId());
        }
        stats.recordCancelled();
      }
    }
  }
}
