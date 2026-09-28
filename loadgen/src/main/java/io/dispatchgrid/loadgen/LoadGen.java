package io.dispatchgrid.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Drives the stack end to end: seeds a moving fleet, submits rides at a fixed rate, waits for the
 * matcher to drain, then prints and writes a measured summary that carries its own provenance.
 */
public final class LoadGen {

  /**
   * Stands in for a provenance field the caller did not supply; a patched figure must not use it.
   */
  static final String UNSPECIFIED = "unspecified";

  public static void main(String[] args) throws Exception {
    Options opt = Options.parse(args);
    List<City> cities = City.first(opt.cities());
    Http http = new Http();

    waitForReady(http, opt);
    JsonNode before = http.getJsonWithRetry(opt.matchingUrl() + "/matching/stats", 5);
    long matched0 = before.get("matched").asLong();
    long unmatched0 = before.get("unmatched").asLong();
    long retries0 = before.get("retries").asLong();
    long dropped0 = before.get("dropped").asLong();

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
    long loadStartedAtEpochMs = System.currentTimeMillis();
    double loadAverageAtStart = systemLoadAverage();
    List<Map<String, Long>> loadSamples = new ArrayList<>();
    loadSamples.add(
        Map.of(
            "epochMs", loadStartedAtEpochMs,
            "ridesSubmitted", rides.submitted.get(),
            "pingsOk", fleet.pingsOk.get()));
    for (int s = 1; s <= opt.durationSeconds(); s++) {
      Thread.sleep(1000);
      // Local successful-send counters only: a slow /matching/stats read must neither delay
      // sampling nor extend the advertised load duration. Final service stats are read below.
      loadSamples.add(
          Map.of(
              "epochMs", System.currentTimeMillis(),
              "ridesSubmitted", rides.submitted.get(),
              "pingsOk", fleet.pingsOk.get()));
      if (s % 10 == 0) {
        System.out.printf(
            "  t=%3ds submitted=%d pings_ok=%d ride_errors=%d ping_errors=%d rides_skipped=%d"
                + " pings_skipped=%d rides_in_flight=%d pings_in_flight=%d%n",
            s,
            rides.submitted.get(),
            fleet.pingsOk.get(),
            rides.errors.get(),
            fleet.pingErrors.get(),
            rides.skipped.get(),
            fleet.pingsSkipped.get(),
            rides.inFlight(),
            fleet.inFlight());
      }
    }
    // Use the final sample before shutdown/draining, never the time when settling finishes.
    long loadStoppedAtEpochMs = loadSamples.getLast().get("epochMs");
    double loadAverageAtEnd = systemLoadAverage();
    rides.stop();
    long runSeconds = Math.max(1, (System.nanoTime() - runStart) / 1_000_000_000L);

    JsonNode stats = settle(http, opt, rides.submitted.get());
    fleet.stop();

    long matched = stats.get("matched").asLong() - matched0;
    long unmatched = stats.get("unmatched").asLong() - unmatched0;
    long matchRetries = stats.get("retries").asLong() - retries0;
    long dropped = stats.get("dropped").asLong() - dropped0;
    JsonNode shardStats = http.getJsonWithRetry(opt.riderUrl() + "/rides/stats", 5);
    Map<String, Map<Integer, Long>> shards = shardDistribution(shardStats);

    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put(
        "provenance",
        provenance(
            loadStartedAtEpochMs, loadStoppedAtEpochMs, loadAverageAtStart, loadAverageAtEnd));
    summary.put("durationSeconds", opt.durationSeconds());
    summary.put("ridesPerSecond", opt.ridesPerSecond());
    // Whole seconds from the start of ride submission to its stop: the divisor of the per-minute
    // rate below, which can differ from the configured duration.
    summary.put("runSeconds", runSeconds);
    summary.put("loadStartedAtEpochMs", loadStartedAtEpochMs);
    summary.put("loadStoppedAtEpochMs", loadStoppedAtEpochMs);
    summary.put("loadSamples", loadSamples);
    summary.put("cities", cities.stream().map(City::name).toList());
    summary.put("cityIds", cities.stream().map(City::id).toList());
    summary.put("drivers", fleet.size());
    summary.put("driversPerCity", opt.driversPerCity());
    summary.put("pingsOk", fleet.pingsOk.get());
    summary.put("pingErrors", fleet.pingErrors.get());
    summary.put("pingRetries", fleet.pingRetries.get());
    summary.put("pingsSkipped", fleet.pingsSkipped.get());
    summary.put("maxInFlightPings", fleet.maxInFlight());
    summary.put("ridesSubmitted", rides.submitted.get());
    summary.put("rideErrors", rides.errors.get());
    summary.put("rideRetries", rides.retries.get());
    summary.put("ridesSkipped", rides.skipped.get());
    summary.put("maxInFlightRides", rides.maxInFlight());
    summary.put("matched", matched);
    summary.put("unmatched", unmatched);
    // A ride that finds no free driver waits in the retry store for its next attempt, so this is
    // where a latency tail comes from. Per pod like the counters above, and reset by a replacement.
    summary.put("matchRetries", matchRetries);
    summary.put("dropped", dropped);
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

    // The text is rendered from the fields above alone and stored beside them, so it can be checked
    // against them. `complete` goes in last, after the text, and only then is the map serialized,
    // once to the file and once as the SUMMARY_JSON line: a summary with the flag was written after
    // every other field was in.
    String text = renderSummary(summary);
    summary.put("summaryText", text);
    summary.put("complete", true);
    Files.writeString(
        Path.of(opt.out()), Http.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(summary));

    System.out.println();
    System.out.print(text);
    System.out.println("SUMMARY_JSON " + toJson(summary));
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

  /**
   * What the numbers were measured on, so a summary quoted elsewhere carries its own origin: the
   * commit and machine the demo path passes in, the clock window of the measured load, and the load
   * average of the kernel this generator shares with the services at either end of that window.
   */
  static Map<String, Object> provenance(
      long startedAtEpochMs, long stoppedAtEpochMs, double loadAtStart, double loadAtEnd) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("commit", env("RUN_COMMIT"));
    out.put("machine", env("RUN_MACHINE"));
    out.put("kernel", kernel());
    out.put("startedAt", utcSeconds(startedAtEpochMs));
    out.put("finishedAt", utcSeconds(stoppedAtEpochMs));
    out.put("loadAverageAtStart", loadAverage(loadAtStart));
    out.put("loadAverageAtEnd", loadAverage(loadAtEnd));
    return out;
  }

  /**
   * The generator runs beside the services, so this is the kernel their latency was measured on.
   */
  private static String kernel() {
    return System.getProperty("os.name", UNSPECIFIED).toLowerCase(Locale.ROOT)
        + "/"
        + System.getProperty("os.arch", UNSPECIFIED)
        + ", "
        + Runtime.getRuntime().availableProcessors()
        + " CPU, JDK "
        + System.getProperty("java.version", UNSPECIFIED);
  }

  private static double systemLoadAverage() {
    return ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
  }

  /** A negative reading means the platform does not expose it; never report that as a number. */
  static String loadAverage(double average) {
    return average < 0 ? "unavailable" : String.format(Locale.ROOT, "%.2f", average);
  }

  private static String utcSeconds(long epochMs) {
    return Instant.ofEpochMilli(epochMs).truncatedTo(ChronoUnit.SECONDS).toString();
  }

  private static String env(String name) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? UNSPECIFIED : value.trim();
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

  /**
   * Renders the printed summary from the stored fields alone, so the text is a function of the
   * numbers stored beside it; scripts/patch-demo-summary.py renders the same lines from those
   * fields and refuses a text that differs. The wildcard types let a summary read back from its
   * JSON, where map keys are strings, render the same text as the maps built in {@link #main}.
   */
  static String renderSummary(Map<String, ?> s) {
    List<?> ids = (List<?>) s.get("cityIds");
    List<?> names = (List<?>) s.get("cities");
    StringJoiner cities = new StringJoiner(", ");
    for (int i = 0; i < ids.size(); i++) {
      cities.add(ids.get(i) + "=" + names.get(i));
    }
    StringJoiner shards = new StringJoiner(" | ");
    ((Map<?, ?>) s.get("tripsByShard"))
        .forEach(
            (shard, perCity) -> {
              StringJoiner trips = new StringJoiner(", ");
              ((Map<?, ?>) perCity)
                  .forEach((city, count) -> trips.add("city " + city + " -> " + count + " trips"));
              shards.add(shard + ": " + trips);
            });
    Map<?, ?> prov = (Map<?, ?>) s.get("provenance");
    StringBuilder out = new StringBuilder();
    line(out, "== dispatchgrid load summary ==");
    line(
        out,
        "run                 %d s at %d rides/s, cities %s",
        s.get("durationSeconds"),
        s.get("ridesPerSecond"),
        cities);
    line(out, "commit              %s", prov.get("commit"));
    line(out, "measured window     %s -> %s", prov.get("startedAt"), prov.get("finishedAt"));
    line(out, "machine             %s; container %s", prov.get("machine"), prov.get("kernel"));
    line(
        out,
        "load average        %s at the start of the load, %s at the end (kernel above)",
        prov.get("loadAverageAtStart"),
        prov.get("loadAverageAtEnd"));
    line(
        out,
        "drivers             %d (%d per city), pings ok=%d errors=%d retries=%d skipped=%d",
        s.get("drivers"),
        s.get("driversPerCity"),
        s.get("pingsOk"),
        s.get("pingErrors"),
        s.get("pingRetries"),
        s.get("pingsSkipped"));
    line(
        out,
        "rides submitted     %d, http errors=%d, retries=%d, skipped=%d, by shard %s",
        s.get("ridesSubmitted"),
        s.get("rideErrors"),
        s.get("rideRetries"),
        s.get("ridesSkipped"),
        s.get("submittedByShard"));
    line(
        out,
        "in-flight bound     %d pings, %d rides; a send past the bound is skipped and counted,"
            + " not queued, and is not an http error",
        s.get("maxInFlightPings"),
        s.get("maxInFlightRides"));
    line(
        out,
        "decided (durable)   %d of %d trip rows, %d matched, %d still requested",
        s.get("durableDecided"),
        s.get("durableTrips"),
        s.get("durableMatched"),
        s.get("durableRequested"));
    line(
        out,
        "matching counters   matched=%d unmatched=%d retried=%d dropped=%d (in process, per"
            + " pod, reset by a rollout)",
        s.get("matched"),
        s.get("unmatched"),
        s.get("matchRetries"),
        s.get("dropped"));
    line(
        out,
        "matches per minute  %d over the %d s run (matching-service trailing 60 s window: %d)",
        s.get("matchesPerMinuteRun"),
        s.get("runSeconds"),
        s.get("matchesPerMinuteWindow"));
    line(
        out,
        "match latency       p50=%d ms  p95=%d ms  p99=%d ms",
        s.get("p50LatencyMs"),
        s.get("p95LatencyMs"),
        s.get("p99LatencyMs"));
    line(out, "shard distribution  %s", shards);
    return out.toString();
  }

  private static void line(StringBuilder out, String format, Object... args) {
    out.append(String.format(Locale.ROOT, format, args)).append('\n');
  }

  private static String toJson(Object o) {
    try {
      return Http.JSON.writeValueAsString(o);
    } catch (IOException e) {
      return "{}";
    }
  }
}
