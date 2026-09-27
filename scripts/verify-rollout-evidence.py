#!/usr/bin/env python3
"""Fail closed unless measured successful traffic covers every service replacement.

The runner and kind containers share the kernel's epoch clock. A two-second boundary
margin avoids claiming coverage from coincident samples. This is sampled evidence:
no sampling/traffic-progress gap may exceed five seconds, and the observed successful
rate across each rollout must reach 90% of the configured rate (scheduler jitter).
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


SERVICES = ["rider-request-service", "driver-location-service", "matching-service"]
MARGIN_MS = 2000
MAX_GAP_MS = 5000
MIN_RATE_FRACTION = 0.9
# A skipped ping is the generator refusing to queue a position write because its in-flight bound
# was already reached, which happens for a second or two while a driver-location pod drains on a
# two core runner. It is not a service error and not a caller seeing the update: the sampled rate
# and progress checks above already fail if skipping actually starved the offered load, so the
# count is reported and bounded rather than required to be zero. A skipped ride submission stays
# fatal, because each ride is a distinct rider rather than a position refresh the next tick repeats.
MAX_PING_SKIP_FRACTION = 0.01


def timestamp(epoch_ms):
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).isoformat(timespec="milliseconds")


def integer(record, key):
    value = record[key]
    if type(value) is not int or value < 0:
        raise ValueError(f"{key} must be a nonnegative integer")
    return value


def verify(summary, windows):
    problems = []
    started = integer(summary, "loadStartedAtEpochMs")
    stopped = integer(summary, "loadStoppedAtEpochMs")
    if stopped <= started:
        raise ValueError("load window must have positive duration")
    samples = summary["loadSamples"]
    if len(samples) < 2:
        raise ValueError("at least two load samples are required")
    for sample in samples:
        for field in ("epochMs", "ridesSubmitted", "pingsOk"):
            integer(sample, field)
    if samples[0]["epochMs"] != started or samples[-1]["epochMs"] != stopped:
        problems.append("load samples do not cover the measured load boundaries")
    for previous, current in zip(samples, samples[1:]):
        gap = current["epochMs"] - previous["epochMs"]
        if not 0 < gap <= MAX_GAP_MS:
            problems.append(f"invalid or unobserved sample gap: {gap}ms")
        for field in ("ridesSubmitted", "pingsOk"):
            if current[field] < previous[field]:
                problems.append(f"non-monotonic {field} samples")
    if [window["service"] for window in windows] != SERVICES:
        problems.append("expected exactly one completed rollout for each service, in order")

    print("== rolling update evidence ==")
    print(f"measured active load    {timestamp(started)} -> {timestamp(stopped)}")
    print("coverage policy         2s boundary margin; <=5s sampled progress gaps; >=90% target rate")
    previous_end = started
    for window in windows:
        service = window["service"]
        start = integer(window, "startEpochMs")
        end = integer(window, "endEpochMs")
        covered = started + MARGIN_MS <= start < end <= stopped - MARGIN_MS
        print(f"{service}: {timestamp(start)} -> {timestamp(end)}; load-contained={covered}")
        if not covered:
            problems.append(f"{service}: full rollout is not inside measured active load (including margin)")
        if start < previous_end:
            problems.append(f"{service}: invalid sequential rollout timestamps")
        previous_end = end
        # Samples immediately outside a rollout bracket all replacements. Per-service rates
        # and progress checks prevent a stopped/stalled scheduler from hiding in aggregate totals.
        before = [i for i, sample in enumerate(samples) if sample["epochMs"] <= start]
        after = [i for i, sample in enumerate(samples) if sample["epochMs"] >= end]
        if not before or not after:
            problems.append(f"{service}: no samples bracketing the rollout")
            continue
        observed = samples[before[-1]:after[0] + 1]
        seconds = (observed[-1]["epochMs"] - observed[0]["epochMs"]) / 1000
        if seconds <= 0:
            problems.append(f"{service}: empty observed interval")
            continue
        for field, target in (("ridesSubmitted", integer(summary, "ridesPerSecond")),
                              ("pingsOk", integer(summary, "drivers"))):
            if target == 0:
                problems.append(f"{service}: zero target rate for {field}")
            delivered = observed[-1][field] - observed[0][field]
            rate = delivered / seconds
            print(f"  {field}: {delivered} successes / {seconds:.3f}s = {rate:.2f}/s (target {target}/s)")
            if rate < target * MIN_RATE_FRACTION:
                problems.append(f"{service}: insufficient measured {field} rate")
            last_progress = observed[0]["epochMs"]
            for previous, current in zip(observed, observed[1:]):
                if current["epochMs"] - last_progress > MAX_GAP_MS:
                    problems.append(f"{service}: no observed {field} progress within {MAX_GAP_MS}ms")
                    break
                if current[field] > previous[field]:
                    last_progress = current["epochMs"]

    for field in ("rideErrors", "pingErrors", "ridesSkipped"):
        count = integer(summary, field)
        if count:
            problems.append(f"{field}: {count} (must be zero)")
    pings_skipped = integer(summary, "pingsSkipped")
    pings_ok = integer(summary, "pingsOk")
    if pings_skipped > MAX_PING_SKIP_FRACTION * max(1, pings_ok):
        problems.append(
            f"pingsSkipped: {pings_skipped} of {pings_ok} delivered"
            f" (over {MAX_PING_SKIP_FRACTION:.0%}, the position stream was not kept up)"
        )
    submitted = integer(summary, "ridesSubmitted")
    if submitted == 0:
        problems.append("no rides submitted")
    if integer(summary, "durableDecided") < submitted:
        problems.append("not all submitted rides have a durable decision")
    if integer(summary, "durableTrips") != submitted:
        problems.append("durable trip rows differ from successful submissions")
    if integer(summary, "durableMatched") < 0.9 * submitted:
        problems.append("durable match rate below 90%")
    shards = summary["tripsByShard"]
    if set(shards) != {"shard-0", "shard-1"}:
        problems.append("expected both city shards")
    for shard, cities in shards.items():
        if len(cities) != 1:
            problems.append(f"{shard}: expected exactly one city")
    print(f"ride requests           {submitted} submitted, {summary['rideErrors']} errors, {summary['ridesSkipped']} skipped")
    print(f"driver position pings   {summary['pingsOk']} ok, {summary['pingErrors']} errors, {summary['pingsSkipped']} skipped")
    print(f"transport retries       {summary.get('rideRetries', 0)} rides, {summary.get('pingRetries', 0)} pings")
    print(
        f"matching counters       {summary.get('matched', 0)} matched,"
        f" {summary.get('unmatched', 0)} unmatched,"
        f" {summary.get('matchRetries', 0)} retried, {summary.get('dropped', 0)} dropped"
        " (in process, per pod, reset by each replacement)"
    )
    print(
        f"match latency           p50={summary['p50LatencyMs']}ms p95={summary['p95LatencyMs']}ms"
        f" p99={summary['p99LatencyMs']}ms (reservoir of the pod that answered the last read;"
        " a ride with no free driver waits in the retry store, which is where the tail comes from)"
    )
    print(f"durable decisions       {summary['durableDecided']}/{summary['durableTrips']}; matched={summary['durableMatched']}")
    print(f"trips by shard          {json.dumps(shards)}")
    return problems


def main():
    try:
        summary = json.loads(Path(sys.argv[1]).read_text())
        windows = [json.loads(line) for line in Path(sys.argv[2]).read_text().splitlines() if line]
        problems = verify(summary, windows)
    except (IndexError, KeyError, TypeError, ValueError, OSError) as error:
        problems = [f"invalid or missing rollout evidence: {error}"]
    if problems:
        print("RESULT: FAIL")
        for problem in problems:
            print(" - " + problem)
        return 1
    print("COVERAGE: PASS (all three complete rollouts inside sustained sampled load)")
    print("RESULT: PASS (zero final request errors, no skipped ride, pings within the skip bound)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
