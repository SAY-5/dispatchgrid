package io.dispatchgrid.common.model;

import java.time.Instant;

/** A rider asking for a trip. Published to the ride-requests topic keyed by city id. */
public record RideRequest(
    String rideId,
    String riderId,
    int cityId,
    double pickupLat,
    double pickupLng,
    double dropoffLat,
    double dropoffLng,
    Instant requestedAt) {}
