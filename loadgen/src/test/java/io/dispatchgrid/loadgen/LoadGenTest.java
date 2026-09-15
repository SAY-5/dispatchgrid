package io.dispatchgrid.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

  @Test
  void totalsDurableStatusesAcrossShardsAndCountsOnlyDecidedRows() throws Exception {
    JsonNode stats =
        Http.JSON.readTree(
            "{\"shard-0\":{\"2:MATCHED\":10,\"2:REQUESTED\":3},"
                + "\"shard-1\":{\"1:MATCHED\":7,\"1:UNMATCHED\":2}}");
    assertThat(LoadGen.statusTotals(stats))
        .containsExactly(
            Map.entry("MATCHED", 17L), Map.entry("REQUESTED", 3L), Map.entry("UNMATCHED", 2L));
    assertThat(LoadGen.decided(stats)).isEqualTo(19L);
  }

  @Test
  void retriesAReadThatFailsOnce() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    AtomicInteger calls = new AtomicInteger();
    server.createContext(
        "/stats",
        exchange -> {
          byte[] body = "{\"matched\":3}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(calls.incrementAndGet() == 1 ? 500 : 200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/stats";
      assertThat(new Http().getJsonWithRetry(url, 3).get("matched").asInt()).isEqualTo(3);
      assertThat(calls.get()).isEqualTo(2);
    } finally {
      server.stop(0);
    }
  }
}
