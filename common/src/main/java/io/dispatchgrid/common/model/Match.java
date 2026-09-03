package io.dispatchgrid.common.model;

import java.time.Instant;

/**
 * A ride paired with a claimed driver. Published to the ride-matches topic keyed by city id. The
 * surge multiplier is the pricing signal for the pickup cell at the moment of the match; 1.0 means
 * no surge.
 */
public record Match(
    String rideId,
    String driverId,
    int cityId,
    double driverDistanceMeters,
    int searchRadiusMeters,
    long matchLatencyMs,
    Instant matchedAt,
    double surgeMultiplier) {}
