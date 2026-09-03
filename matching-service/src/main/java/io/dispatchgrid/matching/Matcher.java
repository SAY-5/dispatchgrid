package io.dispatchgrid.matching;

import io.dispatchgrid.common.geo.RadiusExpansion;
import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.model.RideUnmatched;
import io.dispatchgrid.common.redis.ClaimResult;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.redis.NearbyDriver;
import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Pure matching policy: search nearest-first inside a radius, try to claim each candidate, and
 * widen the radius when the ring is empty or every candidate was taken. Stateless, so it is safe
 * to call from every stream thread concurrently; Redis provides the atomicity.
 */
public class Matcher {
  private final DriverIndex index;
  private final MatchingProperties props;
  private final RadiusExpansion expansion;
  private final Clock clock;

  public Matcher(DriverIndex index, MatchingProperties props) {
    this(index, props, Clock.systemUTC());
  }

  public Matcher(DriverIndex index, MatchingProperties props, Clock clock) {
    this.index = index;
    this.props = props;
    this.expansion = props.expansion();
    this.clock = clock;
  }

  public MatchOutcome match(RideRequest r) {
    int taken = 0;
    for (int radius : expansion.radii()) {
      List<NearbyDriver> candidates =
          index.nearby(
              r.cityId(), r.pickupLat(), r.pickupLng(), radius, props.candidatesPerRadius());
      for (NearbyDriver c : candidates) {
        ClaimResult result = index.claim(r.cityId(), c.driverId(), r.rideId(), props.claimTtl());
        if (result == ClaimResult.CLAIMED) {
          Instant now = clock.instant();
          return MatchOutcome.matched(
              new Match(
                  r.rideId(),
                  c.driverId(),
                  r.cityId(),
                  c.distanceMeters(),
                  radius,
                  Math.max(0, now.toEpochMilli() - r.requestedAt().toEpochMilli()),
                  now));
        }
        if (result == ClaimResult.TAKEN) {
          taken++;
        }
      }
    }
    String reason = taken > 0 ? "all_candidates_taken" : "no_drivers_in_range";
    return MatchOutcome.unmatched(
        new RideUnmatched(r.rideId(), r.cityId(), props.maxRadiusMeters(), reason, clock.instant()));
  }
}
