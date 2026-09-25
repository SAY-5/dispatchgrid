package io.dispatchgrid.loadgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

/** Simulated drivers wandering around each city and pinging their position once a second. */
final class Fleet {
  private static final double SPAWN_RADIUS_M = 5000;
  private static final double FENCE_RADIUS_M = 6500;

  /**
   * Design constant: the most position pings held in flight at once. The default workload is 300
   * drivers per city in two cities, each pinging once a second, so a target that answers within a
   * second never holds more than 600. Two seconds of pings leaves room for ordinary jitter before a
   * tick starts skipping. Without a bound the 10 second request timeout, doubled by the single
   * retry, lets a stalled target hold 12000 pings and their connection buffers at once, which is
   * what exhausted the heap; with it the generator holds at most 1200 whatever the target does.
   */
  static final int MAX_IN_FLIGHT_PINGS = 1200;

  final AtomicLong pingsOk = new AtomicLong();
  final AtomicLong pingErrors = new AtomicLong();
  final AtomicLong pingRetries = new AtomicLong();

  /** Pings not sent because the in-flight bound was already reached when their tick ran. */
  final AtomicLong pingsSkipped = new AtomicLong();

  private final Http http;
  private final String driverUrl;
  private final List<Driver> drivers = new ArrayList<>();
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

  private static final class Driver {
    final String id;
    final City city;
    double north;
    double east;
    double heading;
    final RandomGenerator rnd;

    Driver(String id, City city, RandomGenerator rnd) {
      this.id = id;
      this.city = city;
      this.rnd = rnd;
      double r = SPAWN_RADIUS_M * Math.sqrt(rnd.nextDouble());
      double a = rnd.nextDouble() * 2 * Math.PI;
      this.north = r * Math.cos(a);
      this.east = r * Math.sin(a);
      this.heading = rnd.nextDouble() * 2 * Math.PI;
    }

    void step() {
      heading += (rnd.nextDouble() - 0.5) * 0.6;
      double speed = 8 + rnd.nextDouble() * 7;
      north += speed * Math.cos(heading);
      east += speed * Math.sin(heading);
      if (Math.hypot(north, east) > FENCE_RADIUS_M) {
        heading += Math.PI;
      }
    }

    Map<String, Object> body() {
      double[] p = City.offset(city.lat(), city.lng(), north, east);
      return Map.of("cityId", city.id(), "lat", p[0], "lng", p[1], "status", "AVAILABLE");
    }
  }

  Fleet(Http http, String driverUrl, List<City> cities, int perCity, long seed) {
    this(http, driverUrl, cities, perCity, seed, MAX_IN_FLIGHT_PINGS);
  }

  Fleet(Http http, String driverUrl, List<City> cities, int perCity, long seed, int maxInFlight) {
    this.http = http;
    this.driverUrl = driverUrl;
    this.maxInFlight = maxInFlight;
    this.permits = new Semaphore(maxInFlight);
    for (City c : cities) {
      for (int i = 0; i < perCity; i++) {
        drivers.add(
            new Driver(
                "d-" + c.id() + "-" + i, c, new java.util.Random(seed + c.id() * 100_000L + i)));
      }
    }
  }

  int size() {
    return drivers.size();
  }

  /** Pings in flight right now. */
  int inFlight() {
    return maxInFlight - permits.availablePermits();
  }

  /** One synchronous round of pings; used to seed the index before rides start. */
  void pingAllAndWait() throws InterruptedException {
    CountDownLatch done = new CountDownLatch(drivers.size());
    for (Driver d : drivers) {
      submit(d, done::countDown);
    }
    done.await(30, TimeUnit.SECONDS);
  }

  void start() {
    ticker.scheduleAtFixedRate(this::tick, 1000, 1000, TimeUnit.MILLISECONDS);
  }

  /** One round of pings, one per driver. */
  void tick() {
    for (Driver d : drivers) {
      submit(d, () -> {});
    }
  }

  /**
   * Sends one ping unless the bound is reached, in which case the ping is skipped and counted
   * rather than queued: the position write is an upsert and the next tick sends a fresher one.
   */
  private void submit(Driver d, Runnable done) {
    if (!permits.tryAcquire()) {
      pingsSkipped.incrementAndGet();
      done.run();
      return;
    }
    workers.submit(
        () -> {
          try {
            ping(d);
          } finally {
            permits.release();
            done.run();
          }
        });
  }

  private void ping(Driver d) {
    d.step();
    String url = driverUrl + "/drivers/" + d.id + "/position";
    Object body = d.body();
    try {
      record(http.postJson(url, body));
    } catch (IOException first) {
      // The position write is an idempotent upsert, so one retry on a transport failure
      // (for example a keep-alive connection closed by a draining pod) is safe and counted.
      pingRetries.incrementAndGet();
      try {
        record(http.postJson(url, body));
      } catch (Exception second) {
        pingErrors.incrementAndGet();
      }
    } catch (Exception e) {
      pingErrors.incrementAndGet();
    }
  }

  private void record(int code) {
    if (code / 100 == 2) {
      pingsOk.incrementAndGet();
    } else {
      pingErrors.incrementAndGet();
    }
  }

  void stop() {
    ticker.shutdownNow();
    workers.shutdown();
  }
}
