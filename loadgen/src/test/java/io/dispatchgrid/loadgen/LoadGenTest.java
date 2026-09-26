package io.dispatchgrid.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
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
