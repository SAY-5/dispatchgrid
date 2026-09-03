package io.dispatchgrid.common.shard;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Shard map. Shard index for a city is {@code cityId mod shards.size()} unless the city is pinned
 * in {@code overrides}, which lets an operator move a hot city without changing the modulus.
 */
@ConfigurationProperties(prefix = "dispatchgrid")
public class ShardProperties {
  private List<Shard> shards = new ArrayList<>();
  private Map<Integer, Integer> overrides = new LinkedHashMap<>();
  private boolean migrate = true;

  public List<Shard> getShards() {
    return shards;
  }

  public void setShards(List<Shard> shards) {
    this.shards = shards;
  }

  public Map<Integer, Integer> getOverrides() {
    return overrides;
  }

  public void setOverrides(Map<Integer, Integer> overrides) {
    this.overrides = overrides;
  }

  public boolean isMigrate() {
    return migrate;
  }

  public void setMigrate(boolean migrate) {
    this.migrate = migrate;
  }

  public static class Shard {
    private String name;
    private String url;
    private String username;
    private String password;
    private int maxPoolSize = 10;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public int getMaxPoolSize() {
      return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
    }
  }
}
