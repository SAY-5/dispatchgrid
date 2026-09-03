package io.dispatchgrid.common.model;

import java.time.Instant;

/** A ride paired with a claimed driver. Published to the ride-matches topic keyed by city id. */
public record Match(
    String rideId,
    String driverId,
    int cityId,
    double driverDistanceMeters,
    int searchRadiusMeters,
    long matchLatencyMs,
    Instant matchedAt) {}
