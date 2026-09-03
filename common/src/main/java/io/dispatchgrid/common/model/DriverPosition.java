package io.dispatchgrid.common.model;

import java.time.Instant;

/** A driver heartbeat with a location. Published to the driver-positions topic keyed by city id. */
public record DriverPosition(
    String driverId, int cityId, double lat, double lng, DriverStatus status, Instant reportedAt) {}
