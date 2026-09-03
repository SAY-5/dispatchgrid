package io.dispatchgrid.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

/** Submits ride requests at a steady rate, round-robin across cities. */
final class Rides {
  private static final double PICKUP_RADIUS_M = 4000;

  final AtomicLong submitted = new AtomicLong();
  final AtomicLong errors = new AtomicLong();
  final Map<String, AtomicLong> byShard = new ConcurrentHashMap<>();
  final Map<Integer, AtomicLong> byCity = new ConcurrentHashMap<>();

  private final Http http;
  private final String riderUrl;
  private final List<City> cities;
  private final RandomGenerator rnd;
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
  private long counter;

  Rides(Http http, String riderUrl, List<City> cities, long seed) {
    this.http = http;
    this.riderUrl = riderUrl;
    this.cities = cities;
    this.rnd = new java.util.Random(seed);
  }

  void start(int perSecond) {
    long periodMicros = 1_000_000L / perSecond;
    ticker.scheduleAtFixedRate(
        () -> {
          City c = cities.get((int) (counter++ % cities.size()));
          Map<String, Object> body = nextRequest(c);
          workers.submit(() -> submit(body));
        },
        0,
        periodMicros,
        TimeUnit.MICROSECONDS);
  }

  private Map<String, Object> nextRequest(City c) {
    double r = PICKUP_RADIUS_M * Math.sqrt(rnd.nextDouble());
    double a = rnd.nextDouble() * 2 * Math.PI;
    double[] pickup = City.offset(c.lat(), c.lng(), r * Math.cos(a), r * Math.sin(a));
    double r2 = (PICKUP_RADIUS_M + 2000) * Math.sqrt(rnd.nextDouble());
    double a2 = rnd.nextDouble() * 2 * Math.PI;
    double[] dropoff = City.offset(c.lat(), c.lng(), r2 * Math.cos(a2), r2 * Math.sin(a2));
    return Map.of(
        "riderId", "r-" + c.id() + "-" + rnd.nextInt(100_000),
        "cityId", c.id(),
        "pickupLat", pickup[0],
        "pickupLng", pickup[1],
        "dropoffLat", dropoff[0],
        "dropoffLng", dropoff[1]);
  }

  private void submit(Map<String, Object> body) {
    try {
      JsonNode res = http.postJsonForBody(riderUrl + "/rides", body);
      submitted.incrementAndGet();
      byShard.computeIfAbsent("shard-" + res.get("shard").asInt(), k -> new AtomicLong()).incrementAndGet();
      byCity.computeIfAbsent(res.get("cityId").asInt(), k -> new AtomicLong()).incrementAndGet();
    } catch (Exception e) {
      errors.incrementAndGet();
    }
  }

  void stop() {
    ticker.shutdownNow();
    workers.shutdown();
    try {
      workers.awaitTermination(15, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
