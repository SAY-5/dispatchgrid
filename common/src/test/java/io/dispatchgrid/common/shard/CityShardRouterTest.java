package io.dispatchgrid.common.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

class CityShardRouterTest {

  private static List<DataSource> shards(int n) {
    return java.util.stream.IntStream.range(0, n)
        .mapToObj(i -> (DataSource) new SimpleDriverDataSource())
        .toList();
  }

  @Test
  void mapsCityToShardByModulus() {
    CityShardRouter router = new CityShardRouter(shards(2));
    assertThat(router.shardIndexFor(1)).isEqualTo(1);
    assertThat(router.shardIndexFor(2)).isEqualTo(0);
    assertThat(router.shardIndexFor(3)).isEqualTo(1);
    assertThat(router.shardIndexFor(0)).isEqualTo(0);
  }

  @Test
  void isDeterministicAcrossInstances() {
    CityShardRouter a = new CityShardRouter(shards(3));
    CityShardRouter b = new CityShardRouter(shards(3));
    for (int city = 0; city < 1000; city++) {
      assertThat(a.shardIndexFor(city)).isEqualTo(b.shardIndexFor(city));
      assertThat(a.dataSourceFor(city)).isSameAs(a.shard(a.shardIndexFor(city)));
    }
  }

  @Test
  void negativeCityIdsStayInRange() {
    CityShardRouter router = new CityShardRouter(shards(4));
    assertThat(router.shardIndexFor(-1)).isBetween(0, 3);
    assertThat(router.shardIndexFor(Integer.MIN_VALUE)).isBetween(0, 3);
  }

  @Test
  void overridePinsCityToShard() {
    CityShardRouter router = new CityShardRouter(shards(2), Map.of(7, 0));
    assertThat(router.shardIndexFor(7)).isEqualTo(0);
    assertThat(router.shardIndexFor(9)).isEqualTo(1);
  }

  @Test
  void rejectsBadConfiguration() {
    assertThatThrownBy(() -> new CityShardRouter(List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new CityShardRouter(shards(2), Map.of(1, 5)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing shard");
  }
}
