import type { Rng } from "./rng";

/**
 * Service time model for the browser port. The real matcher's latency is the time between the
 * rider service producing the request and the Streams task claiming a driver: a shard insert,
 * a produce, a poll, one GEOSEARCH and one claim script per candidate tried. In the browser those
 * take microseconds, so the run charges a synthetic service time drawn from a log normal fitted to
 * the measured run (p50 14 ms, p95 53 ms) with a rare tail for group rebalances and GC pauses.
 */
export function drawMatchLatencyMs(rnd: Rng, claimsTried: number, extraQueueMs = 0): number {
  const base = Math.exp(Math.log(13) + 0.78 * rnd.gaussian());
  const perClaim = Math.max(0, claimsTried - 1) * 1.2;
  const tail = rnd.next() < 0.012 ? 120 + rnd.next() * 260 : 0;
  return Math.round(base + perClaim + tail + extraQueueMs);
}

/** Poll to poll latency of a Streams task: commit.interval.ms is 200 in application.yml. */
export function drawPollDelayMs(rnd: Rng): number {
  return 2 + Math.floor(rnd.next() * 6);
}
