package io.dispatchgrid.common.model;

import java.time.Instant;

/**
 * A rider cancelling or a driver completing a trip. Published to the ride-lifecycle topic keyed by
 * city id; the matching service applies it to the claim. {@code driverId} is null when a ride is
 * cancelled before it was matched.
 */
public record TripEvent(
    String rideId, int cityId, String driverId, TripEventType type, Instant at) {}
