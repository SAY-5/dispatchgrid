package io.dispatchgrid.common.shard;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Routes a city id to one of N shards. The mapping is a pure function of the id and the shard
 * count, so every service instance agrees on where a city's rows live without coordination.
 */
public final class CityShardRouter {
  private final List<DataSource> shards;
  private final Map<Integer, Integer> overrides;

  public CityShardRouter(List<DataSource> shards, Map<Integer, Integer> overrides) {
    if (shards.isEmpty()) {
      throw new IllegalArgumentException("at least one shard is required");
    }
    for (var e : overrides.entrySet()) {
      if (e.getValue() < 0 || e.getValue() >= shards.size()) {
        throw new IllegalArgumentException(
            "override for city " + e.getKey() + " points at missing shard " + e.getValue());
      }
    }
    this.shards = List.copyOf(shards);
    this.overrides = Map.copyOf(overrides);
  }

  public CityShardRouter(List<DataSource> shards) {
    this(shards, Collections.emptyMap());
  }

  public int shardCount() {
    return shards.size();
  }

  /** Shard index for a city: the pinned override if present, otherwise {@code cityId mod N}. */
  public int shardIndexFor(int cityId) {
    Integer pinned = overrides.get(cityId);
    return pinned != null ? pinned : Math.floorMod(cityId, shards.size());
  }

  public DataSource dataSourceFor(int cityId) {
    return shards.get(shardIndexFor(cityId));
  }

  public DataSource shard(int index) {
    return shards.get(index);
  }

  public List<DataSource> allShards() {
    return shards;
  }
}
