package io.dispatchgrid.matching.surge;

import static org.assertj.core.api.Assertions.assertThat;

import io.dispatchgrid.common.geo.Geo;
import io.dispatchgrid.common.model.DriverPosition;
import io.dispatchgrid.common.model.DriverStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SurgeTrackerTest {
  static final double LAT = 47.6062;
  static final double LNG = -122.3321;

  static final class StepClock extends Clock {
    final AtomicLong now = new AtomicLong(1_700_000_000_000L);

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(now.get());
    }

    @Override
    public long millis() {
      return now.get();
    }
  }

  final StepClock clock = new StepClock();
  final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  final SurgeTracker surge =
      new SurgeTracker(
          new SurgeProperties(1000, Duration.ofSeconds(60), Duration.ofSeconds(15), 0.5, 3.0, 2),
          registry,
          clock);

  private void driver(String id, int city, double lat, double lng) {
    surge.recordDriver(
        new DriverPosition(id, city, lat, lng, DriverStatus.AVAILABLE, clock.instant()));
  }

  @Test
  void multiplierRisesWhenDemandOutpacesSupplyAndDecaysBack() {
    driver("d1", 1, LAT, LNG);
    driver("d2", 1, LAT, LNG);
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(1.0);

    surge.recordRequest(1, LAT, LNG);
    surge.recordRequest(1, LAT, LNG);
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(1.0);

    for (int i = 0; i < 6; i++) {
      clock.now.addAndGet(5_000);
      surge.recordRequest(1, LAT, LNG);
      driver("d1", 1, LAT, LNG);
      driver("d2", 1, LAT, LNG);
    }
    // 8 requests against 2 drivers: ratio 4, multiplier 1 + 0.5 * 3 = 2.5
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(2.5);
    assertThat(registry.get("dispatchgrid.surge.max").tag("city", "1").gauge().value())
        .isEqualTo(2.5);

    // 35 s later the first burst is out of the 60 s window, so the signal is lower but not gone
    clock.now.addAndGet(35_000);
    driver("d1", 1, LAT, LNG);
    driver("d2", 1, LAT, LNG);
    double decayed = surge.multiplierAt(1, LAT, LNG);
    assertThat(decayed).isLessThan(2.5).isGreaterThan(1.0);

    clock.now.addAndGet(60_000);
    driver("d1", 1, LAT, LNG);
    driver("d2", 1, LAT, LNG);
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(1.0);
    assertThat(surge.snapshot(1)).allSatisfy(c -> assertThat(c.demand()).isZero());
  }

  @Test
  void supplyArrivingInTheCellBringsTheMultiplierDown() {
    driver("d1", 1, LAT, LNG);
    for (int i = 0; i < 4; i++) {
      surge.recordRequest(1, LAT, LNG);
    }
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(2.5);
    for (int i = 0; i < 3; i++) {
      driver("new" + i, 1, LAT, LNG);
    }
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(1.0);
  }

  @Test
  void silentDriversStopCountingAsSupply() {
    driver("d1", 1, LAT, LNG);
    driver("d2", 1, LAT, LNG);
    driver("d3", 1, LAT, LNG);
    driver("d4", 1, LAT, LNG);
    for (int i = 0; i < 4; i++) {
      surge.recordRequest(1, LAT, LNG);
    }
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(1.0);
    clock.now.addAndGet(16_000);
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(2.5);
  }

  @Test
  void cellsAndCitiesAreIndependentAndCapped() {
    double[] elsewhere = Geo.offset(LAT, LNG, 5000, 5000);
    driver("d1", 1, LAT, LNG);
    driver("e1", 1, elsewhere[0], elsewhere[1]);
    driver("x1", 2, LAT, LNG);
    for (int i = 0; i < 40; i++) {
      surge.recordRequest(1, LAT, LNG);
    }
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(3.0);
    assertThat(surge.multiplierAt(1, elsewhere[0], elsewhere[1])).isEqualTo(1.0);
    assertThat(surge.multiplierAt(2, LAT, LNG)).isEqualTo(1.0);
    assertThat(surge.multiplierAt(9, LAT, LNG)).isEqualTo(1.0);
    assertThat(surge.surgingCells(1)).isEqualTo(1);
    assertThat(surge.snapshot(1)).hasSize(2);
    assertThat(surge.snapshot(1).get(0).multiplier()).isEqualTo(3.0);
    assertThat(surge.cityIds()).containsExactly(1, 2);
  }

  @Test
  void busyAndOfflineDriversAreNotSupply() {
    driver("d1", 1, LAT, LNG);
    surge.recordDriver(new DriverPosition("d1", 1, LAT, LNG, DriverStatus.BUSY, clock.instant()));
    surge.recordRequest(1, LAT, LNG);
    surge.recordRequest(1, LAT, LNG);
    assertThat(surge.multiplierAt(1, LAT, LNG)).isEqualTo(1.5);
  }
}
