package io.dispatchgrid.matching.surge;

import io.dispatchgrid.common.geo.GeoCell;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Per-city, per-cell supply and demand computed from the two input streams: ride requests add
 * demand to the pickup cell for a trailing window, driver positions place a driver in a cell until
 * its next ping or until the supply TTL passes. Both sides decay on their own, so a cell's
 * multiplier falls back to 1.0 once the burst of requests ages out or drivers move in. Safe to
 * update from every stream thread; reads are lock-free snapshots.
 */
public class SurgeTracker implements SurgePricing {

  /** One cell's current signal. */
  public record CellSurge(String cell, int demand, int supply, double ratio, double multiplier) {}

  private record DriverSpot(String cell, long seenAt) {}

  private static final class City {
    final Map<String, ConcurrentLinkedDeque<Long>> demand = new ConcurrentHashMap<>();
    final Map<String, DriverSpot> drivers = new ConcurrentHashMap<>();
  }

  private final SurgeProperties props;
  private final Clock clock;
  private final MeterRegistry registry;
  private final Map<Integer, City> cities = new ConcurrentHashMap<>();

  public SurgeTracker(SurgeProperties props, MeterRegistry registry, Clock clock) {
    this.props = props;
    this.registry = registry;
    this.clock = clock;
  }

  public SurgeProperties properties() {
    return props;
  }

  private City city(int cityId) {
    return cities.computeIfAbsent(
        cityId,
        id -> {
          Tags tags = Tags.of("city", Integer.toString(id));
          registry.gauge("dispatchgrid.surge.max", tags, this, t -> t.maxMultiplier(id));
          registry.gauge("dispatchgrid.surge.cells", tags, this, t -> t.surgingCells(id));
          return new City();
        });
  }

  public void recordRequest(int cityId, double lat, double lng) {
    long now = clock.millis();
    String cell = GeoCell.cellOf(lat, lng, props.cellMeters());
    ConcurrentLinkedDeque<Long> q =
        city(cityId).demand.computeIfAbsent(cell, c -> new ConcurrentLinkedDeque<>());
    q.addLast(now);
    trim(q, now);
  }

  public void recordDriver(DriverPosition p) {
    City c = city(p.cityId());
    if (p.status() != DriverStatus.AVAILABLE) {
      c.drivers.remove(p.driverId());
      return;
    }
    String cell = GeoCell.cellOf(p.lat(), p.lng(), props.cellMeters());
    c.drivers.put(p.driverId(), new DriverSpot(cell, clock.millis()));
  }

  @Override
  public double multiplierAt(int cityId, double lat, double lng) {
    City c = cities.get(cityId);
    if (c == null) {
      return 1.0;
    }
    String cell = GeoCell.cellOf(lat, lng, props.cellMeters());
    long now = clock.millis();
    return multiplier(demand(c, cell, now), supply(c, now).getOrDefault(cell, 0));
  }

  /** Every cell in the city that currently has demand or supply, hottest first. */
  public List<CellSurge> snapshot(int cityId) {
    City c = cities.get(cityId);
    if (c == null) {
      return List.of();
    }
    long now = clock.millis();
    Map<String, Integer> supply = supply(c, now);
    Map<String, Integer> demand = new ConcurrentHashMap<>();
    c.demand.forEach(
        (cell, q) -> {
          trim(q, now);
          if (q.isEmpty()) {
            c.demand.remove(cell, q);
          } else {
            demand.put(cell, q.size());
          }
        });
    List<CellSurge> out = new ArrayList<>();
    for (String cell : union(demand.keySet(), supply.keySet())) {
      int d = demand.getOrDefault(cell, 0);
      int s = supply.getOrDefault(cell, 0);
      out.add(new CellSurge(cell, d, s, ratio(d, s), multiplier(d, s)));
    }
    out.sort(
        Comparator.comparingDouble(CellSurge::multiplier)
            .reversed()
            .thenComparing(CellSurge::cell));
    return out;
  }

  public double maxMultiplier(int cityId) {
    return snapshot(cityId).stream().mapToDouble(CellSurge::multiplier).max().orElse(1.0);
  }

  public long surgingCells(int cityId) {
    return snapshot(cityId).stream().filter(s -> s.multiplier() > 1.0).count();
  }

  public List<Integer> cityIds() {
    return cities.keySet().stream().sorted().toList();
  }

  private int demand(City c, String cell, long now) {
    ConcurrentLinkedDeque<Long> q = c.demand.get(cell);
    if (q == null) {
      return 0;
    }
    trim(q, now);
    return q.size();
  }

  private Map<String, Integer> supply(City c, long now) {
    long ttl = props.supplyTtl().toMillis();
    Map<String, Integer> out = new ConcurrentHashMap<>();
    c.drivers.forEach(
        (driver, spot) -> {
          if (now - spot.seenAt() > ttl) {
            c.drivers.remove(driver, spot);
          } else {
            out.merge(spot.cell(), 1, Integer::sum);
          }
        });
    return out;
  }

  private void trim(ConcurrentLinkedDeque<Long> q, long now) {
    long window = props.demandWindow().toMillis();
    Long head;
    while ((head = q.peekFirst()) != null && now - head > window) {
      q.pollFirst();
    }
  }

  private static double ratio(int demand, int supply) {
    return demand / (double) Math.max(supply, 1);
  }

  double multiplier(int demand, int supply) {
    if (demand < props.minDemand()) {
      return 1.0;
    }
    double raw = 1.0 + props.slope() * (ratio(demand, supply) - 1.0);
    double clamped = Math.max(1.0, Math.min(props.maxMultiplier(), raw));
    return Math.round(clamped * 100.0) / 100.0;
  }

  private static List<String> union(Iterable<String> a, Iterable<String> b) {
    java.util.TreeSet<String> s = new java.util.TreeSet<>();
    a.forEach(s::add);
    b.forEach(s::add);
    return new ArrayList<>(s);
  }
}
