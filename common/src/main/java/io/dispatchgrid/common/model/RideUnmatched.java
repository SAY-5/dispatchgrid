package io.dispatchgrid.common.model;

import java.time.Instant;

/**
 * A ride that found no claimable driver within the maximum radius after {@code attempts} passes.
 */
public record RideUnmatched(
    String rideId,
    int cityId,
    int maxRadiusMeters,
    String reason,
    int attempts,
    Instant decidedAt) {}
