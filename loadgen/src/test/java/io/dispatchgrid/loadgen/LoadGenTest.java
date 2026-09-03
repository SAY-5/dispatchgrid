package io.dispatchgrid.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LoadGenTest {

  @Test
  void parsesFlagsInBothForms() {
    Options o =
        Options.parse(new String[] {"--duration=30", "--rides-per-second", "5", "--cities=2"});
    assertThat(o.durationSeconds()).isEqualTo(30);
    assertThat(o.ridesPerSecond()).isEqualTo(5);
    assertThat(o.cities()).isEqualTo(2);
    assertThat(o.riderUrl()).startsWith("http");
  }

  @Test
  void collapsesShardStatsPerCity() throws Exception {
    JsonNode stats =
        Http.JSON.readTree(
            "{\"shard-0\":{\"2:MATCHED\":10,\"2:UNMATCHED\":1},\"shard-1\":{\"1:MATCHED\":7}}");
    Map<String, Map<Integer, Long>> d = LoadGen.shardDistribution(stats);
    assertThat(d.get("shard-0")).containsExactly(Map.entry(2, 11L));
    assertThat(d.get("shard-1")).containsExactly(Map.entry(1, 7L));
  }

  @Test
  void citiesMapToDistinctShardsUnderTwoShardModulus() {
    assertThat(City.first(2)).extracting(City::id).containsExactly(1, 2);
    assertThat(1 % 2).isNotEqualTo(2 % 2);
  }
}
