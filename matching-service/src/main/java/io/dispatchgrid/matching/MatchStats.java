package io.dispatchgrid.matching;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cheap in-process counters behind {@code GET /matching/stats}: totals, a trailing 60 second
 * match rate, and latency percentiles over a fixed-size reservoir of recent matches. Micrometer
 * meters are updated in parallel for scraping.
 */
public class MatchStats {
  static final int RESERVOIR = 8192;
  static final long WINDOW_MS = 60_000;

  private final Clock clock;
  private final AtomicLong matched = new AtomicLong();
  private final AtomicLong unmatched = new AtomicLong();
  private final AtomicLong dropped = new AtomicLong();
  private final AtomicLong firstMatchAt = new AtomicLong();
  private final ConcurrentLinkedDeque<Long> recent = new ConcurrentLinkedDeque<>();
  private final long[] latencies = new long[RESERVOIR];
  private final AtomicInteger cursor = new AtomicInteger();
  private final AtomicLong latencyCount = new AtomicLong();
  private final Timer timer;
  private final Counter matchedCounter;
  private final Counter unmatchedCounter;

  public MatchStats(MeterRegistry registry, Clock clock) {
    this.clock = clock;
    this.timer =
        Timer.builder("dispatchgrid.match.latency")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
    this.matchedCounter = registry.counter("dispatchgrid.match.matched");
    this.unmatchedCounter = registry.counter("dispatchgrid.match.unmatched");
  }

  public void recordMatch(long latencyMs) {
    long now = clock.millis();
    matched.incrementAndGet();
    matchedCounter.increment();
    firstMatchAt.compareAndSet(0, now);
    recent.addLast(now);
    trim(now);
    int i = Math.floorMod(cursor.getAndIncrement(), RESERVOIR);
    latencies[i] = latencyMs;
    latencyCount.incrementAndGet();
    timer.record(latencyMs, TimeUnit.MILLISECONDS);
  }

  public void recordUnmatched() {
    unmatched.incrementAndGet();
    unmatchedCounter.increment();
  }

  public void recordDropped() {
    dropped.incrementAndGet();
  }

  private void trim(long now) {
    Long head;
    while ((head = recent.peekFirst()) != null && now - head > WINDOW_MS) {
      recent.pollFirst();
    }
  }

  public Map<String, Object> snapshot() {
    long now = clock.millis();
    trim(now);
    long total = matched.get();
    long first = firstMatchAt.get();
    double minutes = first == 0 ? 0 : Math.max(now - first, 1) / 60_000.0;
    long[] sample = sortedSample();
    Map<String, Object> out = new TreeMap<>();
    out.put("matched", total);
    out.put("unmatched", unmatched.get());
    out.put("dropped", dropped.get());
    out.put("matchesPerMinute", recent.size());
    out.put("matchesPerMinuteOverall", minutes == 0 ? 0 : Math.round(total / minutes));
    out.put("windowSeconds", WINDOW_MS / 1000);
    out.put("secondsSinceFirstMatch", first == 0 ? 0 : (now - first) / 1000);
    out.put("latencySamples", sample.length);
    out.put("p50LatencyMs", percentile(sample, 0.50));
    out.put("p95LatencyMs", percentile(sample, 0.95));
    out.put("p99LatencyMs", percentile(sample, 0.99));
    return out;
  }

  private long[] sortedSample() {
    int n = (int) Math.min(latencyCount.get(), RESERVOIR);
    long[] copy = Arrays.copyOf(latencies, n);
    Arrays.sort(copy);
    return copy;
  }

  static long percentile(long[] sorted, double p) {
    if (sorted.length == 0) {
      return 0;
    }
    int idx = (int) Math.ceil(p * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
  }
}
