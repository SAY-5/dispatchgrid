package io.dispatchgrid.matching;

import io.dispatchgrid.common.geo.Geo;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.dispatchgrid.common.redis.ClaimResult;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.redis.NearbyDriver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Deterministic stand-in for Redis so the matching policy can be tested without containers. */
final class InMemoryDriverIndex implements DriverIndex {
  record Entry(int cityId, double lat, double lng) {}

  final Map<String, Entry> drivers = new ConcurrentHashMap<>();
  final Map<String, String> claims = new ConcurrentHashMap<>();
  final List<String> claimAttempts = new ArrayList<>();
  int claimCalls;
  int nearbyCalls;

  void add(String id, int cityId, double lat, double lng) {
    drivers.put(id, new Entry(cityId, lat, lng));
  }

  @Override
  public void upsert(DriverPosition p, Duration heartbeatTtl) {
    if (p.status() == DriverStatus.OFFLINE) {
      drivers.remove(p.driverId());
    } else {
      add(p.driverId(), p.cityId(), p.lat(), p.lng());
    }
  }

  @Override
  public List<NearbyDriver> nearby(
      int cityId, double lat, double lng, int radiusMeters, int limit) {
    nearbyCalls++;
    return drivers.entrySet().stream()
        .filter(e -> e.getValue().cityId() == cityId && !claims.containsKey(e.getKey()))
        .map(
            e ->
                new NearbyDriver(
                    e.getKey(),
                    Geo.haversineMeters(lat, lng, e.getValue().lat(), e.getValue().lng())))
        .filter(n -> n.distanceMeters() <= radiusMeters)
        .sorted(Comparator.comparingDouble(NearbyDriver::distanceMeters))
        .limit(limit)
        .toList();
  }

  @Override
  public synchronized ClaimResult claim(int cityId, String driverId, String rideId, Duration ttl) {
    claimCalls++;
    claimAttempts.add(driverId);
    if (!drivers.containsKey(driverId)) {
      return ClaimResult.STALE;
    }
    return claims.putIfAbsent(driverId, rideId) == null ? ClaimResult.CLAIMED : ClaimResult.TAKEN;
  }

  @Override
  public boolean release(int cityId, String driverId, String rideId) {
    return claims.remove(driverId, rideId);
  }

  @Override
  public String claimedBy(int cityId, String driverId) {
    return claims.get(driverId);
  }

  @Override
  public long size(int cityId) {
    return drivers.values().stream().filter(e -> e.cityId() == cityId).count();
  }
}
