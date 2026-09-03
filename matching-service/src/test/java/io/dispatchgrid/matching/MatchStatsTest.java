package io.dispatchgrid.matching;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MatchStatsTest {

  /** Clock whose millis can be advanced by the test. */
  static final class StepClock extends Clock {
    final AtomicLong now = new AtomicLong(1_700_000_000_000L);

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
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

  @Test
  void percentilesUseNearestRank() {
    long[] sorted = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertThat(MatchStats.percentile(sorted, 0.5)).isEqualTo(5);
    assertThat(MatchStats.percentile(sorted, 0.95)).isEqualTo(10);
    assertThat(MatchStats.percentile(new long[] {7}, 0.99)).isEqualTo(7);
    assertThat(MatchStats.percentile(new long[0], 0.5)).isZero();
  }

  @Test
  void trailingWindowDropsOldMatches() {
    StepClock clock = new StepClock();
    MatchStats stats = new MatchStats(new SimpleMeterRegistry(), clock);
    for (int i = 0; i < 100; i++) {
      stats.recordMatch(10 + i);
      clock.now.addAndGet(100);
    }
    Map<String, Object> s = stats.snapshot();
    assertThat(s.get("matched")).isEqualTo(100L);
    assertThat(s.get("matchesPerMinute")).isEqualTo(100);
    assertThat(s.get("p50LatencyMs")).isEqualTo(59L);
    assertThat(s.get("p95LatencyMs")).isEqualTo(104L);

    clock.now.addAndGet(55_000);
    s = stats.snapshot();
    assertThat(s.get("matched")).isEqualTo(100L);
    assertThat((Integer) s.get("matchesPerMinute")).isLessThan(100).isGreaterThan(0);

    clock.now.addAndGet(60_000);
    assertThat(stats.snapshot().get("matchesPerMinute")).isEqualTo(0);
  }

  @Test
  void countsUnmatchedAndDroppedSeparately() {
    MatchStats stats = new MatchStats(new SimpleMeterRegistry(), new StepClock());
    stats.recordUnmatched();
    stats.recordDropped();
    stats.recordDropped();
    Map<String, Object> s = stats.snapshot();
    assertThat(s.get("unmatched")).isEqualTo(1L);
    assertThat(s.get("dropped")).isEqualTo(2L);
    assertThat(s.get("matched")).isEqualTo(0L);
  }
}
