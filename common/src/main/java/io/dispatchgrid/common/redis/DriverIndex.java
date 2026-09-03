package io.dispatchgrid.common.redis;

import io.dispatchgrid.common.model.DriverPosition;
import java.time.Duration;
import java.util.List;

/** Geospatial view of drivers per city plus atomic claims. */
public interface DriverIndex {

  /** Records a position and heartbeat. Offline drivers are removed from the index. */
  void upsert(DriverPosition position, Duration heartbeatTtl);

  /** Nearest drivers within {@code radiusMeters}, nearest first, at most {@code limit}. */
  List<NearbyDriver> nearby(int cityId, double lat, double lng, int radiusMeters, int limit);

  /** Atomically reserves the driver for the ride. Exactly one ride can win a driver. */
  ClaimResult claim(int cityId, String driverId, String rideId, Duration claimTtl);

  /** Frees a driver claimed by {@code rideId}; other rides' claims are left alone. */
  boolean release(int cityId, String driverId, String rideId);

  /** The ride currently holding the driver, or null. */
  String claimedBy(int cityId, String driverId);

  /** Number of drivers in the city index. */
  long size(int cityId);
}
