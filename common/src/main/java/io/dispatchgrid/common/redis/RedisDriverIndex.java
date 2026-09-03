package io.dispatchgrid.common.redis;

import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.domain.geo.GeoReference;

/**
 * Redis-backed index: one GEO set of available drivers per city, one heartbeat hash per driver
 * with a TTL so silent drivers age out, and a claim key per driver set with NX so a driver is
 * never handed to two rides. Each operation is a single Lua script, so claim, stale detection,
 * and set membership never race.
 */
public class RedisDriverIndex implements DriverIndex {

  /**
   * KEYS[1] geo set, KEYS[2] heartbeat hash, KEYS[3] claim key. ARGV[1] driver id, ARGV[2] lng,
   * ARGV[3] lat, ARGV[4] status, ARGV[5] reported at, ARGV[6] heartbeat ttl ms. A driver that is
   * currently claimed keeps its heartbeat but stays out of the searchable set.
   */
  private static final String UPSERT_LUA =
      """
      redis.call('HSET', KEYS[2], 'status', ARGV[4], 'lat', ARGV[3], 'lng', ARGV[2], 'reported_at', ARGV[5])
      redis.call('PEXPIRE', KEYS[2], ARGV[6])
      if redis.call('EXISTS', KEYS[3]) == 1 then
        redis.call('ZREM', KEYS[1], ARGV[1])
        return 0
      end
      redis.call('GEOADD', KEYS[1], ARGV[2], ARGV[3], ARGV[1])
      return 1
      """;

  /**
   * KEYS[1] claim key, KEYS[2] heartbeat hash, KEYS[3] geo set. ARGV[1] ride id, ARGV[2] ttl ms,
   * ARGV[3] driver id. Returns 1 claimed, 0 taken, -1 stale. A successful claim removes the driver
   * from the searchable set until the claim expires or is released.
   */
  private static final String CLAIM_LUA =
      """
      if redis.call('EXISTS', KEYS[2]) == 0 then
        redis.call('ZREM', KEYS[3], ARGV[3])
        redis.call('DEL', KEYS[1])
        return -1
      end
      if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
        redis.call('ZREM', KEYS[3], ARGV[3])
        return 1
      end
      return 0
      """;

  /**
   * KEYS[1] claim key, KEYS[2] heartbeat hash, KEYS[3] geo set. ARGV[1] ride id, ARGV[2] driver
   * id. Returns 1 if released; the driver is put back at its last reported position.
   */
  private static final String RELEASE_LUA =
      """
      if redis.call('GET', KEYS[1]) == ARGV[1] then
        redis.call('DEL', KEYS[1])
        local lat = redis.call('HGET', KEYS[2], 'lat')
        local lng = redis.call('HGET', KEYS[2], 'lng')
        if lat and lng then
          redis.call('GEOADD', KEYS[3], lng, lat, ARGV[2])
        end
        return 1
      end
      return 0
      """;

  private final StringRedisTemplate redis;
  private final DefaultRedisScript<Long> upsertScript;
  private final DefaultRedisScript<Long> claimScript;
  private final DefaultRedisScript<Long> releaseScript;

  public RedisDriverIndex(StringRedisTemplate redis) {
    this.redis = redis;
    this.upsertScript = new DefaultRedisScript<>(UPSERT_LUA, Long.class);
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
    redis.execute(
        upsertScript,
        List.of(geo, hash, claimKey(p.cityId(), p.driverId())),
        p.driverId(),
        Double.toString(p.lng()),
        Double.toString(p.lat()),
        p.status().name(),
        p.reportedAt().toString(),
        Long.toString(heartbeatTtl.toMillis()));
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
    Long r =
        redis.execute(
            releaseScript,
            List.of(claimKey(cityId, driverId), driverKey(cityId, driverId), geoKey(cityId)),
            rideId,
            driverId);
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
