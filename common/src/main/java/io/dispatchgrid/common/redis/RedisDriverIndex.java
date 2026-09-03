package io.dispatchgrid.common.redis;

import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.domain.geo.GeoReference;

/**
 * Redis-backed index: one GEO set per city, one heartbeat hash per driver with a TTL so silent
 * drivers age out, and a claim key per driver set with NX so a driver is never handed to two
 * rides. Claim and stale detection run inside a single Lua script so they are atomic.
 */
public class RedisDriverIndex implements DriverIndex {

  /**
   * KEYS[1] claim key, KEYS[2] heartbeat hash, KEYS[3] geo set. ARGV[1] ride id, ARGV[2] ttl ms,
   * ARGV[3] driver id. Returns 1 claimed, 0 taken, -1 stale.
   */
  private static final String CLAIM_LUA =
      """
      if redis.call('EXISTS', KEYS[2]) == 0 then
        redis.call('ZREM', KEYS[3], ARGV[3])
        redis.call('DEL', KEYS[1])
        return -1
      end
      if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
        return 1
      end
      return 0
      """;

  /** KEYS[1] claim key. ARGV[1] ride id. Returns 1 if released. */
  private static final String RELEASE_LUA =
      """
      if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
      end
      return 0
      """;

  private final StringRedisTemplate redis;
  private final DefaultRedisScript<Long> claimScript;
  private final DefaultRedisScript<Long> releaseScript;

  public RedisDriverIndex(StringRedisTemplate redis) {
    this.redis = redis;
    this.claimScript = new DefaultRedisScript<>(CLAIM_LUA, Long.class);
    this.releaseScript = new DefaultRedisScript<>(RELEASE_LUA, Long.class);
  }

  public static String geoKey(int cityId) {
    return "drivers:geo:" + cityId;
  }

  public static String driverKey(int cityId, String driverId) {
    return "driver:" + cityId + ":" + driverId;
  }

  public static String claimKey(int cityId, String driverId) {
    return "claim:" + cityId + ":" + driverId;
  }

  @Override
  public void upsert(DriverPosition p, Duration heartbeatTtl) {
    String geo = geoKey(p.cityId());
    String hash = driverKey(p.cityId(), p.driverId());
    if (p.status() == DriverStatus.OFFLINE) {
      redis.opsForGeo().remove(geo, p.driverId());
      redis.delete(hash);
      return;
    }
    redis.executePipelined(
        (org.springframework.data.redis.core.RedisCallback<Object>)
            conn -> {
              var ser = redis.getStringSerializer();
              conn.geoCommands()
                  .geoAdd(
                      ser.serialize(geo), new Point(p.lng(), p.lat()), ser.serialize(p.driverId()));
              conn.hashCommands()
                  .hMSet(
                      ser.serialize(hash),
                      Map.of(
                          ser.serialize("status"), ser.serialize(p.status().name()),
                          ser.serialize("lat"), ser.serialize(Double.toString(p.lat())),
                          ser.serialize("lng"), ser.serialize(Double.toString(p.lng())),
                          ser.serialize("reported_at"), ser.serialize(p.reportedAt().toString())));
              conn.keyCommands().pExpire(ser.serialize(hash), heartbeatTtl.toMillis());
              return null;
            });
  }

  @Override
  public List<NearbyDriver> nearby(int cityId, double lat, double lng, int radiusMeters, int limit) {
    GeoResults<RedisGeoCommands.GeoLocation<String>> results =
        redis
            .opsForGeo()
            .search(
                geoKey(cityId),
                GeoReference.fromCoordinate(lng, lat),
                new Distance(radiusMeters, RedisGeoCommands.DistanceUnit.METERS),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs()
                    .includeDistance()
                    .sortAscending()
                    .limit(limit));
    List<NearbyDriver> out = new ArrayList<>();
    if (results == null) {
      return out;
    }
    for (GeoResult<RedisGeoCommands.GeoLocation<String>> r : results) {
      out.add(new NearbyDriver(r.getContent().getName(), r.getDistance().in(RedisGeoCommands.DistanceUnit.METERS).getValue()));
    }
    return out;
  }

  @Override
  public ClaimResult claim(int cityId, String driverId, String rideId, Duration claimTtl) {
    Long r =
        redis.execute(
            claimScript,
            List.of(claimKey(cityId, driverId), driverKey(cityId, driverId), geoKey(cityId)),
            rideId,
            Long.toString(claimTtl.toMillis()),
            driverId);
    if (r == null) {
      return ClaimResult.TAKEN;
    }
    return switch (r.intValue()) {
      case 1 -> ClaimResult.CLAIMED;
      case -1 -> ClaimResult.STALE;
      default -> ClaimResult.TAKEN;
    };
  }

  @Override
  public boolean release(int cityId, String driverId, String rideId) {
    Long r = redis.execute(releaseScript, List.of(claimKey(cityId, driverId)), rideId);
    return r != null && r == 1;
  }

  @Override
  public String claimedBy(int cityId, String driverId) {
    return redis.opsForValue().get(claimKey(cityId, driverId));
  }

  @Override
  public long size(int cityId) {
    Long n = redis.opsForZSet().zCard(geoKey(cityId));
    return n == null ? 0 : n;
  }
}
