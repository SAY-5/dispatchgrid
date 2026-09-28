package io.dispatchgrid.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LoadGenTest {

  @Test
  void recordsActualActiveLoadBoundariesAndSuccessfulTrafficSamples(@TempDir Path temporary)
      throws Exception {
    AtomicInteger rides = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext(
        "/",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          String reply;
          if (path.equals("/rides")) {
            rides.incrementAndGet();
            reply = "{\"shard\":1,\"cityId\":1}";
          } else if (path.equals("/rides/stats")) {
            reply = "{\"shard-1\":{\"1:MATCHED\":" + rides.get() + "}}";
          } else if (path.equals("/matching/stats")) {
            reply =
                "{\"matched\":0,\"unmatched\":0,\"retries\":0,\"dropped\":0,\"matchesPerMinute\":0,"
                    + "\"p50LatencyMs\":0,\"p95LatencyMs\":0,\"p99LatencyMs\":0}";
          } else {
            reply = "{\"status\":\"UP\"}";
          }
          exchange.getRequestBody().readAllBytes();
          byte[] body = reply.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    Process process = null;
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort();
      Path summary = temporary.resolve("summary.json");
      Path log = temporary.resolve("loadgen.log");
      long before = System.currentTimeMillis();
      ProcessBuilder builder =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  System.getProperty("java.class.path"),
                  LoadGen.class.getName(),
                  "--rider-url=" + url,
                  "--driver-url=" + url,
                  "--matching-url=" + url,
                  "--duration=2",
                  "--rides-per-second=10",
                  "--drivers-per-city=1",
                  "--cities=1",
                  "--out=" + summary)
              .redirectErrorStream(true)
              .redirectOutput(log.toFile());
      builder.environment().put("RUN_COMMIT", "abc1234");
      builder.environment().put("RUN_MACHINE", "a test host");
      process = builder.start();
      assertThat(process.waitFor(20, TimeUnit.SECONDS)).isTrue();
      assertThat(process.exitValue()).withFailMessage(Files.readString(log)).isZero();
      JsonNode output = Http.JSON.readTree(Files.readString(summary));
      assertThat(output.has("loadStartedAtEpochMs")).isTrue();
      long started = output.get("loadStartedAtEpochMs").asLong();
      long stopped = output.get("loadStoppedAtEpochMs").asLong();
      assertThat(started).isBetween(before, System.currentTimeMillis());
      assertThat(stopped - started).isBetween(1900L, 10000L);
      JsonNode samples = output.get("loadSamples");
      assertThat(samples.size()).isEqualTo(3);
      assertThat(samples.get(0).get("epochMs").asLong()).isEqualTo(started);
      assertThat(samples.get(2).get("epochMs").asLong()).isEqualTo(stopped);
      assertThat(samples.get(2).get("ridesSubmitted").asLong()).isGreaterThan(0);
      assertThat(samples.get(2).get("pingsOk").asLong())
          .isGreaterThan(samples.get(0).get("pingsOk").asLong());
      assertThat(output.get("ridesPerSecond").asInt()).isEqualTo(10);
      JsonNode provenance = output.get("provenance");
      assertThat(provenance.get("commit").asText()).isEqualTo("abc1234");
      assertThat(provenance.get("machine").asText()).isEqualTo("a test host");
      assertThat(provenance.get("kernel").asText()).contains(System.getProperty("os.arch"));
      assertThat(Instant.parse(provenance.get("startedAt").asText()).toEpochMilli())
          .isEqualTo(started / 1000 * 1000);
      assertThat(Instant.parse(provenance.get("finishedAt").asText()).toEpochMilli())
          .isEqualTo(stopped / 1000 * 1000);
      assertThat(provenance.get("loadAverageAtStart").asText())
          .matches("\\d+\\.\\d\\d|unavailable");
      assertThat(provenance.get("loadAverageAtEnd").asText()).matches("\\d+\\.\\d\\d|unavailable");
      assertThat(output.get("complete").asBoolean()).isTrue();
      assertThat(output.get("summaryText").asText())
          .startsWith("== dispatchgrid load summary ==")
          .contains("commit              abc1234")
          .contains("machine             a test host; container ");
      assertThat(output.get("runSeconds").asLong()).isBetween(2L, 10L);
      assertThat(output.get("cityIds").get(0).asInt()).isEqualTo(1);
      assertThat(output.get("driversPerCity").asInt()).isEqualTo(1);
      assertThat(output.get("maxInFlightPings").asInt()).isEqualTo(Fleet.MAX_IN_FLIGHT_PINGS);
      assertThat(output.get("maxInFlightRides").asInt()).isEqualTo(Rides.MAX_IN_FLIGHT_RIDES);
      // Every figure in the text comes from a stored field: read back, the fields render it
      // exactly.
      Map<String, Object> stored =
          Http.JSON.readValue(
              Files.readString(summary), new TypeReference<Map<String, Object>>() {});
      assertThat(LoadGen.renderSummary(stored)).isEqualTo(stored.get("summaryText"));
      assertThat(List.copyOf(stored.keySet()).getLast()).isEqualTo("complete");
      // scripts/compose-demo.sh publishes the printed SUMMARY_JSON line, not the file, so the line
      // must carry the same fields in the same order, `complete` last.
      List<String> printed =
          Files.readAllLines(log).stream().filter(l -> l.startsWith("SUMMARY_JSON ")).toList();
      assertThat(printed).hasSize(1);
      Map<String, Object> line =
          Http.JSON.readValue(
              printed.getFirst().substring("SUMMARY_JSON ".length()),
              new TypeReference<Map<String, Object>>() {});
      assertThat(line).isEqualTo(stored);
      assertThat(List.copyOf(line.keySet())).isEqualTo(List.copyOf(stored.keySet()));
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
      server.stop(0);
    }
  }

  @Test
  void reportsALoadAverageThePlatformWillNotGiveAsUnavailable() {
    assertThat(LoadGen.loadAverage(-1)).isEqualTo("unavailable");
    assertThat(LoadGen.loadAverage(1.234)).isEqualTo("1.23");
  }

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

  @Test
  void skipsPingsBeyondTheInFlightBoundAndCountsThem() throws Exception {
    Semaphore answer = new Semaphore(0);
    AtomicInteger arrived = new AtomicInteger();
    HttpServer server = stallingServer("/drivers", answer, arrived, "{}");
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort();
      Fleet fleet = new Fleet(new Http(), url, City.first(1), 3, 1, 2);

      fleet.tick();
      assertThat(fleet.pingsSkipped.get()).isEqualTo(1);
      waitUntil(() -> arrived.get() == 2);
      assertThat(fleet.inFlight()).isEqualTo(2);

      answer.release(2);
      waitUntil(() -> fleet.inFlight() == 0);
      assertThat(fleet.pingsOk.get()).isEqualTo(2);

      fleet.tick();
      assertThat(fleet.pingsSkipped.get()).isEqualTo(2);
      waitUntil(() -> arrived.get() == 4);
      answer.release(2);
      waitUntil(() -> fleet.inFlight() == 0);
      assertThat(fleet.pingsOk.get()).isEqualTo(4);
      assertThat(fleet.pingErrors.get()).isZero();
      fleet.stop();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void skipsRidesBeyondTheInFlightBoundAndCountsThem() throws Exception {
    Semaphore answer = new Semaphore(0);
    AtomicInteger arrived = new AtomicInteger();
    HttpServer server = stallingServer("/rides", answer, arrived, "{\"shard\":0,\"cityId\":1}");
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort();
      Rides rides = new Rides(new Http(), url, City.first(1), 7, 1);

      rides.tick();
      rides.tick();
      assertThat(rides.skipped.get()).isEqualTo(1);
      waitUntil(() -> arrived.get() == 1);
      assertThat(rides.inFlight()).isEqualTo(1);

      answer.release();
      waitUntil(() -> rides.inFlight() == 0);
      assertThat(rides.submitted.get()).isEqualTo(1);

      rides.tick();
      assertThat(rides.skipped.get()).isEqualTo(1);
      answer.release();
      waitUntil(() -> rides.inFlight() == 0);
      assertThat(rides.submitted.get()).isEqualTo(2);
      assertThat(rides.errors.get()).isZero();
      rides.stop();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void retriesARideOnceWhenTheConnectionClosesWithoutAResponse() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    AtomicInteger calls = new AtomicInteger();
    server.createContext(
        "/rides",
        exchange -> {
          if (calls.incrementAndGet() == 1) {
            exchange.close();
            return;
          }
          byte[] body = "{\"shard\":0,\"cityId\":1}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    try {
      Rides rides =
          new Rides(
              new Http(), "http://127.0.0.1:" + server.getAddress().getPort(), City.first(1), 7);
      rides.tick();
      waitUntil(() -> rides.submitted.get() + rides.errors.get() == 1);
      assertThat(rides.submitted.get()).isEqualTo(1);
      assertThat(rides.retries.get()).isEqualTo(1);
      assertThat(rides.errors.get()).isZero();
      assertThat(calls.get()).isEqualTo(2);
      rides.stop();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void countsAnErrorStatusWithoutRetrying() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    AtomicInteger calls = new AtomicInteger();
    server.createContext(
        "/rides",
        exchange -> {
          calls.incrementAndGet();
          exchange.sendResponseHeaders(503, -1);
          exchange.close();
        });
    server.start();
    try {
      Rides rides =
          new Rides(
              new Http(), "http://127.0.0.1:" + server.getAddress().getPort(), City.first(1), 7);
      rides.tick();
      waitUntil(() -> rides.errors.get() == 1);
      assertThat(rides.retries.get()).isZero();
      assertThat(rides.submitted.get()).isZero();
      assertThat(calls.get()).isEqualTo(1);
      rides.stop();
    } finally {
      server.stop(0);
    }
  }

  /** Answers each request with 200 and the given body once a permit is released. */
  private static HttpServer stallingServer(
      String path, Semaphore answer, AtomicInteger arrived, String reply) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext(
        path,
        exchange -> {
          arrived.incrementAndGet();
          try {
            answer.tryAcquire(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          byte[] body = reply.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    return server;
  }

  private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("condition not met within 5 s");
      }
      Thread.sleep(5);
    }
  }
}
