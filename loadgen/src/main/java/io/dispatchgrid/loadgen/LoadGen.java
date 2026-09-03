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
    JsonNode before = http.getJson(opt.matchingUrl() + "/matching/stats");
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
        JsonNode now = http.getJson(opt.matchingUrl() + "/matching/stats");
        System.out.printf(
            "  t=%3ds submitted=%d matched=%d unmatched=%d ride_errors=%d ping_errors=%d%n",
            s,
            rides.submitted.get(),
            now.get("matched").asLong() - matched0,
            now.get("unmatched").asLong() - unmatched0,
            rides.errors.get(),
            fleet.pingErrors.get());
      }
    }
    rides.stop();
    long runSeconds = Math.max(1, (System.nanoTime() - runStart) / 1_000_000_000L);

    JsonNode stats = settle(http, opt, rides.submitted.get(), matched0, unmatched0);
    fleet.stop();

    long matched = stats.get("matched").asLong() - matched0;
    long unmatched = stats.get("unmatched").asLong() - unmatched0;
    JsonNode shardStats = http.getJson(opt.riderUrl() + "/rides/stats");
    Map<String, Map<Integer, Long>> shards = shardDistribution(shardStats);

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("durationSeconds", opt.durationSeconds());
    summary.put("cities", cities.stream().map(City::name).toList());
    summary.put("drivers", fleet.size());
    summary.put("pingsOk", fleet.pingsOk.get());
    summary.put("pingErrors", fleet.pingErrors.get());
    summary.put("ridesSubmitted", rides.submitted.get());
    summary.put("rideErrors", rides.errors.get());
    summary.put("matched", matched);
    summary.put("unmatched", unmatched);
    summary.put("matchesPerMinuteRun", Math.round(matched * 60.0 / runSeconds));
    summary.put("matchesPerMinuteWindow", stats.get("matchesPerMinute").asLong());
    summary.put("p50LatencyMs", stats.get("p50LatencyMs").asLong());
    summary.put("p95LatencyMs", stats.get("p95LatencyMs").asLong());
    summary.put("p99LatencyMs", stats.get("p99LatencyMs").asLong());
    summary.put("submittedByShard", new TreeMap<>(rides.byShard.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))));
    summary.put("tripsByShard", shards);
    Files.writeString(Path.of(opt.out()), Http.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(summary));

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

  /** Keeps polling until every submitted ride has a decision or the settle window passes. */
  private static JsonNode settle(
      Http http, Options opt, long submitted, long matched0, long unmatched0) throws Exception {
    JsonNode stats = http.getJson(opt.matchingUrl() + "/matching/stats");
    for (int i = 0; i < opt.settleSeconds(); i++) {
      long decided =
          stats.get("matched").asLong() - matched0 + stats.get("unmatched").asLong() - unmatched0;
      if (decided >= submitted) {
        break;
      }
      Thread.sleep(1000);
      stats = http.getJson(opt.matchingUrl() + "/matching/stats");
    }
    return stats;
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
  private static void print(Map<String, Object> s, List<City> cities, Options opt, long runSeconds) {
    String cityNames = cities.stream().map(c -> c.id() + "=" + c.name()).collect(Collectors.joining(", "));
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
    System.out.printf("run                 %d s at %d rides/s, cities %s%n", opt.durationSeconds(), opt.ridesPerSecond(), cityNames);
    System.out.printf("drivers             %d (%d per city), pings ok=%d errors=%d%n", s.get("drivers"), opt.driversPerCity(), s.get("pingsOk"), s.get("pingErrors"));
    System.out.printf("rides submitted     %d, http errors=%d, by shard %s%n", s.get("ridesSubmitted"), s.get("rideErrors"), s.get("submittedByShard"));
    System.out.printf("matched             %d%n", s.get("matched"));
    System.out.printf("unmatched           %d%n", s.get("unmatched"));
    System.out.printf("matches per minute  %d over the %d s run (matching-service trailing 60 s window: %d)%n", s.get("matchesPerMinuteRun"), runSeconds, s.get("matchesPerMinuteWindow"));
    System.out.printf("match latency       p50=%d ms  p95=%d ms  p99=%d ms%n", s.get("p50LatencyMs"), s.get("p95LatencyMs"), s.get("p99LatencyMs"));
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
