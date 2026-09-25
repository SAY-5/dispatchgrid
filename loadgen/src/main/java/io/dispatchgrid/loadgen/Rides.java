package io.dispatchgrid.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

/** Submits ride requests at a steady rate, round-robin across cities. */
final class Rides {
  private static final double PICKUP_RADIUS_M = 4000;

  /**
   * Design constant: the most ride submissions held in flight at once. The default workload submits
   * 10 rides a second and a submission is held for at most the 10 second request timeout, so 100 is
   * the whole offered load with every request stalled to its timeout. Each ride is a distinct rider
   * rather than a refresh that the next tick repeats, so the bound is set at the full load and only
   * trips when a submission hangs past its timeout (the timeout covers the response headers, not a
   * stalled body): a skip marks a hung submission, not a slow one.
   */
  static final int MAX_IN_FLIGHT_RIDES = 100;

  final AtomicLong submitted = new AtomicLong();
  final AtomicLong errors = new AtomicLong();

  /** Rides not sent because the in-flight bound was already reached when their tick ran. */
  final AtomicLong skipped = new AtomicLong();

  final Map<String, AtomicLong> byShard = new ConcurrentHashMap<>();
  final Map<Integer, AtomicLong> byCity = new ConcurrentHashMap<>();

  private final Http http;
  private final String riderUrl;
  private final List<City> cities;
  private final RandomGenerator rnd;
  private final int maxInFlight;
  private final Semaphore permits;
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  // Daemon, so an unexpected failure on the main thread cannot leave this process alive with
  // nothing driving it. Before this, a crash left the job running until activeDeadlineSeconds.
  private final ScheduledExecutorService ticker =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "loadgen-ticker");
            thread.setDaemon(true);
            return thread;
          });
  private long counter;

  Rides(Http http, String riderUrl, List<City> cities, long seed) {
    this(http, riderUrl, cities, seed, MAX_IN_FLIGHT_RIDES);
  }

  Rides(Http http, String riderUrl, List<City> cities, long seed, int maxInFlight) {
    this.http = http;
    this.riderUrl = riderUrl;
    this.cities = cities;
    this.rnd = new java.util.Random(seed);
    this.maxInFlight = maxInFlight;
    this.permits = new Semaphore(maxInFlight);
  }

  void start(int perSecond) {
    long periodMicros = 1_000_000L / perSecond;
    ticker.scheduleAtFixedRate(this::tick, 0, periodMicros, TimeUnit.MICROSECONDS);
  }

  /** Submissions in flight right now. */
  int inFlight() {
    return maxInFlight - permits.availablePermits();
  }

  /**
   * One ride for the next city in round-robin order. If the bound is reached the ride is skipped
   * and counted rather than queued, so a stalled target cannot grow the set of live requests.
   */
  void tick() {
    City c = cities.get((int) (counter++ % cities.size()));
    if (!permits.tryAcquire()) {
      skipped.incrementAndGet();
      return;
    }
    Map<String, Object> body = nextRequest(c);
    workers.submit(
        () -> {
          try {
            submit(body);
          } finally {
            permits.release();
          }
        });
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
      byShard
          .computeIfAbsent("shard-" + res.get("shard").asInt(), k -> new AtomicLong())
          .incrementAndGet();
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
