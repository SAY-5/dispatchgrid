package io.dispatchgrid.loadgen;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

/** Simulated drivers wandering around each city and pinging their position once a second. */
final class Fleet {
  private static final double SPAWN_RADIUS_M = 5000;
  private static final double FENCE_RADIUS_M = 6500;

  final AtomicLong pingsOk = new AtomicLong();
  final AtomicLong pingErrors = new AtomicLong();

  private final Http http;
  private final String driverUrl;
  private final List<Driver> drivers = new ArrayList<>();
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();

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
    this.http = http;
    this.driverUrl = driverUrl;
    for (City c : cities) {
      for (int i = 0; i < perCity; i++) {
        drivers.add(new Driver("d-" + c.id() + "-" + i, c, new java.util.Random(seed + c.id() * 100_000L + i)));
      }
    }
  }

  int size() {
    return drivers.size();
  }

  /** One synchronous round of pings; used to seed the index before rides start. */
  void pingAllAndWait() throws InterruptedException {
    CountDownLatch done = new CountDownLatch(drivers.size());
    for (Driver d : drivers) {
      workers.submit(
          () -> {
            try {
              ping(d);
            } finally {
              done.countDown();
            }
          });
    }
    done.await(30, TimeUnit.SECONDS);
  }

  void start() {
    ticker.scheduleAtFixedRate(
        () -> {
          for (Driver d : drivers) {
            workers.submit(() -> ping(d));
          }
        },
        1000,
        1000,
        TimeUnit.MILLISECONDS);
  }

  private void ping(Driver d) {
    d.step();
    try {
      int code = http.postJson(driverUrl + "/drivers/" + d.id + "/position", d.body());
      if (code / 100 == 2) {
        pingsOk.incrementAndGet();
      } else {
        pingErrors.incrementAndGet();
      }
    } catch (Exception e) {
      pingErrors.incrementAndGet();
    }
  }

  void stop() {
    ticker.shutdownNow();
    workers.shutdown();
  }
}
