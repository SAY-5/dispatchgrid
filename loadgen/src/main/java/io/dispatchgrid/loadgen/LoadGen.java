package io.dispatchgrid.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Drives the stack end to end: seeds a moving fleet, submits rides at a fixed rate, waits for the
 * matcher to drain, then prints and writes a measured summary.
 */
public final class LoadGen {

  public static void main(String[] args) throws Exception {
    Options opt = Options.parse(args);
    List<City> cities = City.first(opt.cities());
    Http http = new Http();

    waitForReady(http, opt);
    JsonNode before = http.getJsonWithRetry(opt.matchingUrl() + "/matching/stats", 5);
    long matched0 = before.get("matched").asLong();
    long unmatched0 = before.get("unmatched").asLong();

    Fleet fleet = new Fleet(http, opt.driverUrl(), cities, opt.driversPerCity(), 42);
    System.out.printf("seeding %d drivers across %d cities%n", fleet.size(), cities.size());
    fleet.pingAllAndWait();
    fleet.pingAllAndWait();
    fleet.start();

    Rides rides = new Rides(http, opt.riderUrl(), cities, 7);
    System.out.printf(
        "submitting %d rides/s for %d s%n", opt.ridesPerSecond(), opt.durationSeconds());
    long runStart = System.nanoTime();
    rides.start(opt.ridesPerSecond());
    for (int s = 1; s <= opt.durationSeconds(); s++) {
      Thread.sleep(1000);
      if (s % 10 == 0) {
        // This read only prints a progress line. The measured counters are rides.errors and
        // fleet.pingErrors, so a stats read that times out while a pod is being replaced must
        // never end the run. Like every other read it is issued from this thread and waited on
        // before the next, so at most one read is ever in flight.
        try {
          JsonNode now = http.getJson(opt.matchingUrl() + "/matching/stats");
          System.out.printf(
              "  t=%3ds submitted=%d matched=%d unmatched=%d ride_errors=%d ping_errors=%d"
                  + " rides_skipped=%d pings_skipped=%d rides_in_flight=%d pings_in_flight=%d%n",
              s,
              rides.submitted.get(),
              now.get("matched").asLong() - matched0,
              now.get("unmatched").asLong() - unmatched0,
              rides.errors.get(),
              fleet.pingErrors.get(),
              rides.skipped.get(),
              fleet.pingsSkipped.get(),
              rides.inFlight(),
              fleet.inFlight());
        } catch (IOException e) {
          System.out.printf(
              "  t=%3ds submitted=%d ride_errors=%d ping_errors=%d rides_skipped=%d"
                  + " pings_skipped=%d rides_in_flight=%d pings_in_flight=%d"
                  + " (stats read failed: %s)%n",
              s,
              rides.submitted.get(),
              rides.errors.get(),
              fleet.pingErrors.get(),
              rides.skipped.get(),
              fleet.pingsSkipped.get(),
              rides.inFlight(),
              fleet.inFlight(),
              e.getMessage());
        }
      }
    }
    rides.stop();
    long runSeconds = Math.max(1, (System.nanoTime() - runStart) / 1_000_000_000L);

    JsonNode stats = settle(http, opt, rides.submitted.get());
    fleet.stop();

    long matched = stats.get("matched").asLong() - matched0;
    long unmatched = stats.get("unmatched").asLong() - unmatched0;
    JsonNode shardStats = http.getJsonWithRetry(opt.riderUrl() + "/rides/stats", 5);
    Map<String, Map<Integer, Long>> shards = shardDistribution(shardStats);

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("durationSeconds", opt.durationSeconds());
    summary.put("cities", cities.stream().map(City::name).toList());
    summary.put("drivers", fleet.size());
    summary.put("pingsOk", fleet.pingsOk.get());
    summary.put("pingErrors", fleet.pingErrors.get());
    summary.put("pingRetries", fleet.pingRetries.get());
    summary.put("pingsSkipped", fleet.pingsSkipped.get());
    summary.put("ridesSubmitted", rides.submitted.get());
    summary.put("rideErrors", rides.errors.get());
    summary.put("ridesSkipped", rides.skipped.get());
    summary.put("matched", matched);
    summary.put("unmatched", unmatched);
    summary.put("matchesPerMinuteRun", Math.round(matched * 60.0 / runSeconds));
    summary.put("matchesPerMinuteWindow", stats.get("matchesPerMinute").asLong());
    summary.put("p50LatencyMs", stats.get("p50LatencyMs").asLong());
    summary.put("p95LatencyMs", stats.get("p95LatencyMs").asLong());
    summary.put("p99LatencyMs", stats.get("p99LatencyMs").asLong());
    summary.put(
        "submittedByShard",
        new TreeMap<>(
            rides.byShard.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))));
    summary.put("tripsByShard", shards);
    Map<String, Long> byStatus = statusTotals(shardStats);
    long durableTrips = byStatus.values().stream().mapToLong(Long::longValue).sum();
    summary.put("tripsByStatus", byStatus);
    summary.put("durableTrips", durableTrips);
    summary.put("durableRequested", byStatus.getOrDefault("REQUESTED", 0L));
    summary.put("durableDecided", durableTrips - byStatus.getOrDefault("REQUESTED", 0L));
    summary.put(
        "durableMatched",
        byStatus.getOrDefault("MATCHED", 0L) + byStatus.getOrDefault("COMPLETED", 0L));
    Files.writeString(
        Path.of(opt.out()), Http.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(summary));

    print(summary, cities, opt, runSeconds);
    System.exit(rides.errors.get() == 0 && fleet.pingErrors.get() == 0 ? 0 : 2);
  }

  private static void waitForReady(Http http, Options opt) throws InterruptedException {
    String[] urls = {
      opt.riderUrl() + "/actuator/health/readiness",
      opt.driverUrl() + "/actuator/health/readiness",
      opt.matchingUrl() + "/actuator/health/readiness"
    };
    for (String url : urls) {
      for (int attempt = 0; ; attempt++) {
        try {
          if ("UP".equals(http.getJson(url).get("status").asText())) {
            break;
          }
        } catch (IOException ignored) {
          // not up yet
        }
        if (attempt >= 120) {
          throw new IllegalStateException("not ready after 120 s: " + url);
        }
        Thread.sleep(1000);
      }
    }
  }

  /**
   * Keeps polling until every submitted ride has a decision or the settle window passes. The
   * decision count comes from the trip rows in the city shards rather than from the
   * matching-service counters, which are in process and per pod and so are reset by a rolling
   * update: polling those can never converge once a pod has been replaced.
   */
  private static JsonNode settle(Http http, Options opt, long submitted) throws Exception {
    for (int i = 0; i < opt.settleSeconds(); i++) {
      if (decided(http.getJsonWithRetry(opt.riderUrl() + "/rides/stats", 5)) >= submitted) {
        break;
      }
      Thread.sleep(1000);
    }
    return http.getJsonWithRetry(opt.matchingUrl() + "/matching/stats", 5);
  }

  /** Trip rows that are no longer REQUESTED, summed across shards. */
  static long decided(JsonNode shardStats) {
    Map<String, Long> byStatus = statusTotals(shardStats);
    long total = byStatus.values().stream().mapToLong(Long::longValue).sum();
    return total - byStatus.getOrDefault("REQUESTED", 0L);
  }

  /** Totals per durable trip status across every shard. */
  static Map<String, Long> statusTotals(JsonNode shardStats) {
    Map<String, Long> out = new TreeMap<>();
    shardStats
        .fields()
        .forEachRemaining(
            shard ->
                shard
                    .getValue()
                    .fields()
                    .forEachRemaining(
                        e ->
                            out.merge(e.getKey().split(":")[1], e.getValue().asLong(), Long::sum)));
    return out;
  }

  /** Collapses "city:status" counts into trips per city per shard. */
  static Map<String, Map<Integer, Long>> shardDistribution(JsonNode shardStats) {
    Map<String, Map<Integer, Long>> out = new TreeMap<>();
    shardStats
        .fields()
        .forEachRemaining(
            shard -> {
              Map<Integer, Long> perCity = new TreeMap<>();
              shard
                  .getValue()
                  .fields()
                  .forEachRemaining(
                      e -> {
                        int city = Integer.parseInt(e.getKey().split(":")[0]);
                        perCity.merge(city, e.getValue().asLong(), Long::sum);
                      });
              out.put(shard.getKey(), perCity);
            });
    return out;
  }

  @SuppressWarnings("unchecked")
  private static void print(
      Map<String, Object> s, List<City> cities, Options opt, long runSeconds) {
    String cityNames =
        cities.stream().map(c -> c.id() + "=" + c.name()).collect(Collectors.joining(", "));
    StringBuilder shards = new StringBuilder();
    ((Map<String, Map<Integer, Long>>) s.get("tripsByShard"))
        .forEach(
            (shard, perCity) -> {
              if (shards.length() > 0) {
                shards.append(" | ");
              }
              shards.append(shard).append(": ");
              shards.append(
                  perCity.entrySet().stream()
                      .map(e -> "city " + e.getKey() + " -> " + e.getValue() + " trips")
                      .collect(Collectors.joining(", ")));
            });
    System.out.println();
    System.out.println("== dispatchgrid load summary ==");
    System.out.printf(
        "run                 %d s at %d rides/s, cities %s%n",
        opt.durationSeconds(), opt.ridesPerSecond(), cityNames);
    System.out.printf(
        "drivers             %d (%d per city), pings ok=%d errors=%d retries=%d skipped=%d%n",
        s.get("drivers"),
        opt.driversPerCity(),
        s.get("pingsOk"),
        s.get("pingErrors"),
        s.get("pingRetries"),
        s.get("pingsSkipped"));
    System.out.printf(
        "rides submitted     %d, http errors=%d, skipped=%d, by shard %s%n",
        s.get("ridesSubmitted"),
        s.get("rideErrors"),
        s.get("ridesSkipped"),
        s.get("submittedByShard"));
    System.out.printf(
        "in-flight bound     %d pings, %d rides; a send past the bound is skipped and counted, not"
            + " queued, and is not an http error%n",
        Fleet.MAX_IN_FLIGHT_PINGS, Rides.MAX_IN_FLIGHT_RIDES);
    System.out.printf(
        "decided (durable)   %d of %d trip rows, %d matched, %d still requested%n",
        s.get("durableDecided"),
        s.get("durableTrips"),
        s.get("durableMatched"),
        s.get("durableRequested"));
    System.out.printf(
        "matching counters   matched=%d unmatched=%d (in process, per pod, reset by a rollout)%n",
        s.get("matched"), s.get("unmatched"));
    System.out.printf(
        "matches per minute  %d over the %d s run (matching-service trailing 60 s window: %d)%n",
        s.get("matchesPerMinuteRun"), runSeconds, s.get("matchesPerMinuteWindow"));
    System.out.printf(
        "match latency       p50=%d ms  p95=%d ms  p99=%d ms%n",
        s.get("p50LatencyMs"), s.get("p95LatencyMs"), s.get("p99LatencyMs"));
    System.out.printf("shard distribution  %s%n", shards);
    System.out.println("SUMMARY_JSON " + toJson(s));
  }

  private static String toJson(Object o) {
    try {
      return Http.JSON.writeValueAsString(o);
    } catch (IOException e) {
      return "{}";
    }
  }
}
