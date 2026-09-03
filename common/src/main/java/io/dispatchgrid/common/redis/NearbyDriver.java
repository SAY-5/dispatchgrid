package io.dispatchgrid.common.redis;

/** A GEOSEARCH hit, sorted nearest first by the index. */
public record NearbyDriver(String driverId, double distanceMeters) {}
