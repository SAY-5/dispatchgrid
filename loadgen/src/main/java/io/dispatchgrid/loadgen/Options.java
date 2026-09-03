package io.dispatchgrid.loadgen;

import java.util.HashMap;
import java.util.Map;

/** Command line flags with defaults tuned for the docker-compose demo. */
public record Options(
    String riderUrl,
    String driverUrl,
    String matchingUrl,
    int durationSeconds,
    int ridesPerSecond,
    int driversPerCity,
    int cities,
    int settleSeconds,
    String out) {

  public static Options parse(String[] args) {
    Map<String, String> kv = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        String key = args[i].substring(2);
        int eq = key.indexOf('=');
        if (eq >= 0) {
          kv.put(key.substring(0, eq), key.substring(eq + 1));
        } else if (i + 1 < args.length) {
          kv.put(key, args[++i]);
        }
      }
    }
    return new Options(
        kv.getOrDefault("rider-url", env("RIDER_URL", "http://localhost:8081")),
        kv.getOrDefault("driver-url", env("DRIVER_URL", "http://localhost:8082")),
        kv.getOrDefault("matching-url", env("MATCHING_URL", "http://localhost:8083")),
        Integer.parseInt(kv.getOrDefault("duration", env("DURATION_SECONDS", "60"))),
        Integer.parseInt(kv.getOrDefault("rides-per-second", env("RIDES_PER_SECOND", "10"))),
        Integer.parseInt(kv.getOrDefault("drivers-per-city", env("DRIVERS_PER_CITY", "300"))),
        Integer.parseInt(kv.getOrDefault("cities", env("CITIES", "2"))),
        Integer.parseInt(kv.getOrDefault("settle", env("SETTLE_SECONDS", "15"))),
        kv.getOrDefault("out", env("SUMMARY_OUT", "loadgen-summary.json")));
  }

  private static String env(String name, String fallback) {
    String v = System.getenv(name);
    return v == null || v.isBlank() ? fallback : v;
  }
}
