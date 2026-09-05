package io.dispatchgrid.matching;

import static org.assertj.core.api.Assertions.assertThat;

import io.dispatchgrid.common.geo.Geo;
import io.dispatchgrid.common.model.RideRequest;
import io.dispatchgrid.common.redis.ClaimResult;
import io.dispatchgrid.common.redis.DriverIndex;
import io.dispatchgrid.common.redis.NearbyDriver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class MatcherTest {
  static final double LAT = 47.6062;
  static final double LNG = -122.3321;
  static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  private final InMemoryDriverIndex index = new InMemoryDriverIndex();
  private final MatchingProperties props =
      new MatchingProperties(1000, 2.0, 8000, 3, Duration.ofSeconds(20));

  private Matcher matcher(long nowOffsetMs) {
    return new Matcher(index, props, Clock.fixed(T0.plusMillis(nowOffsetMs), ZoneOffset.UTC));
  }

  private static RideRequest ride(String id, int city) {
    return new RideRequest(id, "rider", city, LAT, LNG, LAT + 0.01, LNG, T0);
  }

  private void driver(String id, int city, double northMeters) {
    double[] p = Geo.offset(LAT, LNG, northMeters, 0);
    index.add(id, city, p[0], p[1]);
  }

  @Test
  void picksTheNearestDriverInTheFirstRing() {
    driver("far", 1, 900);
    driver("near", 1, 200);
    MatchOutcome out = matcher(45).match(ride("r1", 1));
    assertThat(out.isMatched()).isTrue();
    assertThat(out.match().driverId()).isEqualTo("near");
    assertThat(out.match().searchRadiusMeters()).isEqualTo(1000);
    assertThat(out.match().matchLatencyMs()).isEqualTo(45);
    assertThat(out.match().driverDistanceMeters()).isBetween(190.0, 210.0);
    assertThat(index.claimedBy(1, "near")).isEqualTo("r1");
    assertThat(index.nearbyCalls).isEqualTo(1);
  }

  @Test
  void stampsTheSurgeMultiplierOfThePickupCellOnTheMatch() {
    driver("d", 1, 200);
    Matcher m =
        new Matcher(
            index,
            props,
            (city, lat, lng) -> city == 1 ? 1.8 : 1.0,
            Clock.fixed(T0, ZoneOffset.UTC));
    assertThat(m.match(ride("r1", 1)).match().surgeMultiplier()).isEqualTo(1.8);
  }

  @Test
  void matchesWithoutSurgeWhenNoPricingIsWired() {
    driver("d", 1, 200);
    assertThat(matcher(0).match(ride("r1", 1)).match().surgeMultiplier()).isEqualTo(1.0);
  }

  @Test
  void expandsRadiusUntilADriverIsFound() {
    driver("d", 1, 3500);
    MatchOutcome out = matcher(0).match(ride("r1", 1));
    assertThat(out.isMatched()).isTrue();
    assertThat(out.match().searchRadiusMeters()).isEqualTo(4000);
    assertThat(index.nearbyCalls).isEqualTo(3);
  }

  @Test
  void reportsUnmatchedBeyondTheCap() {
    driver("d", 1, 9000);
    MatchOutcome out = matcher(0).match(ride("r1", 1));
    assertThat(out.isMatched()).isFalse();
    assertThat(out.unmatched().reason()).isEqualTo("no_drivers_in_range");
    assertThat(out.unmatched().maxRadiusMeters()).isEqualTo(8000);
    assertThat(index.nearbyCalls).isEqualTo(4);
  }

  @Test
  void neverCrossesCities() {
    driver("other", 2, 100);
    assertThat(matcher(0).match(ride("r1", 1)).isMatched()).isFalse();
  }

  @Test
  void skipsDriversAlreadyClaimedAndTakesTheNextCandidate() {
    driver("a", 1, 100);
    driver("b", 1, 300);
    driver("c", 1, 500);
    assertThat(matcher(0).match(ride("r1", 1)).match().driverId()).isEqualTo("a");
    assertThat(matcher(0).match(ride("r2", 1)).match().driverId()).isEqualTo("b");
    assertThat(matcher(0).match(ride("r3", 1)).match().driverId()).isEqualTo("c");
    assertThat(matcher(0).match(ride("r4", 1)).unmatched().reason())
        .isEqualTo("no_drivers_in_range");
  }

  @Test
  void fallsThroughWhenEveryCandidateIsTakenBetweenSearchAndClaim() {
    DriverIndex racy =
        new DriverIndex() {
          int calls;

          @Override
          public void upsert(io.dispatchgrid.common.model.DriverPosition p, Duration ttl) {}

          @Override
          public List<NearbyDriver> nearby(int c, double lat, double lng, int r, int limit) {
            return List.of(new NearbyDriver("x", 10), new NearbyDriver("y", 20));
          }

          @Override
          public ClaimResult claim(int c, String d, String ride, Duration ttl) {
            calls++;
            return ClaimResult.TAKEN;
          }

          @Override
          public boolean release(int c, String d, String r) {
            return false;
          }

          @Override
          public String claimedBy(int c, String d) {
            return "someone";
          }

          @Override
          public long size(int c) {
            return 2;
          }
        };
    MatchOutcome out = new Matcher(racy, props, Clock.systemUTC()).match(ride("r1", 1));
    assertThat(out.isMatched()).isFalse();
    assertThat(out.unmatched().reason()).isEqualTo("all_candidates_taken");
  }

  @Test
  void concurrentRidesNeverShareADriver() throws Exception {
    int drivers = 40;
    for (int i = 0; i < drivers; i++) {
      driver("d" + i, 1, 50 + i * 20);
    }
    Matcher m = matcher(0);
    ExecutorService pool = Executors.newFixedThreadPool(8);
    List<Future<MatchOutcome>> results =
        IntStream.range(0, drivers)
            .mapToObj(i -> pool.submit(() -> m.match(ride("r" + i, 1))))
            .toList();
    List<String> assigned = new java.util.ArrayList<>();
    for (Future<MatchOutcome> f : results) {
      MatchOutcome o = f.get();
      assertThat(o.isMatched()).isTrue();
      assigned.add(o.match().driverId());
    }
    pool.shutdown();
    assertThat(assigned).doesNotHaveDuplicates().hasSize(drivers);
  }
}
