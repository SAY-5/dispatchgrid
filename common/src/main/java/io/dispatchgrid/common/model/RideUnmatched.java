package io.dispatchgrid.common.model;

import java.time.Instant;

/** A ride that found no claimable driver within the maximum radius. */
public record RideUnmatched(
    String rideId, int cityId, int maxRadiusMeters, String reason, Instant decidedAt) {}
