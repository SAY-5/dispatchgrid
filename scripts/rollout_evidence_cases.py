#!/usr/bin/env python3
"""Drive scripts/rollout_evidence.py with runs that must pass and runs that must fail.

Every case is a whole run summary plus a rollout window. The gate decides whether the run proves
what the README's evidence block claims, so each case pins one way a run can look green while
proving less: a rollout that outlasts the load, a generator that skipped submissions at its
in-flight bound, a run that never offered the load the job asked for, a summary that does not
record what was asked. Run it with: python3 scripts/rollout_evidence_cases.py
"""
import json
import pathlib
import subprocess
import sys

GOOD = {
    "durationSeconds": 240, "ridesPerSecond": 10, "driversPerCity": 300, "drivers": 600,
    "cities": ["austin", "seattle"],
    "pingsOk": 144000, "pingErrors": 0, "pingRetries": 0, "pingsSkipped": 0,
    "ridesSubmitted": 2400, "rideErrors": 0, "rideRetries": 0, "ridesSkipped": 0,
    "matched": 1200, "unmatched": 0, "matchesPerMinuteRun": 300, "matchesPerMinuteWindow": 300,
    "p50LatencyMs": 20, "p95LatencyMs": 120, "p99LatencyMs": 270,
    "submittedByShard": {"shard-0": 1200, "shard-1": 1200},
    "tripsByShard": {"shard-0": {"2": 1200}, "shard-1": {"1": 1200}},
    "tripsByStatus": {"MATCHED": 2400}, "durableTrips": 2400, "durableRequested": 0,
    "durableDecided": 2400, "durableMatched": 2400,
}
# The summary of CI run 36201543211, which the earlier gate passed and the README quoted: 60s of
# load while the staged rollout took 96s, so the last replacement ran with nothing driving it.
OLD_RUN = {
    "durationSeconds": 60, "cities": ["austin", "seattle"], "drivers": 600,
    "pingsOk": 37200, "pingErrors": 0, "pingRetries": 0, "pingsSkipped": 0,
    "ridesSubmitted": 606, "rideErrors": 0, "rideRetries": 0, "ridesSkipped": 0,
    "matched": 303, "unmatched": 0, "matchesPerMinuteRun": 303, "matchesPerMinuteWindow": 303,
    "p50LatencyMs": 19, "p95LatencyMs": 123, "p99LatencyMs": 273,
    "submittedByShard": {"shard-0": 303, "shard-1": 303},
    "tripsByShard": {"shard-0": {"2": 303}, "shard-1": {"1": 303}},
    "tripsByStatus": {"MATCHED": 606}, "durableTrips": 606, "durableRequested": 0,
    "durableDecided": 606, "durableMatched": 606,
}

GATE = pathlib.Path(__file__).with_name("rollout_evidence.py")


def run(summary, roll=96, roll_from=10, roll_to=106):
    r = subprocess.run(
        [sys.executable, str(GATE), json.dumps(summary), str(roll), str(roll_from), str(roll_to)],
        capture_output=True,
        text=True,
    )
    return r.returncode, r.stdout

def case(name, expect_rc, summary, **kw):
    rc, out = run(summary, **kw)
    verdict = [l for l in out.split("\n") if l.startswith("RESULT") or l.startswith(" - ")]
    ok = "OK " if rc == expect_rc else "WRONG "
    print(f"{ok}{name}: rc={rc} (expected {expect_rc})")
    for v in verdict: print("      " + v)
    return rc == expect_rc

def alter(base, **kw):
    d = dict(base); d.update(kw); return d

results = [
    case("a full run with the rollout inside the load window passes", 0, GOOD),
    case("the run this README quoted, 60s load with a 96s rollout, now FAILS", 1, OLD_RUN),
    case("a rollout that outlasts the load window FAILS", 1, GOOD, roll=250, roll_from=10, roll_to=260),
    case("one skipped ride submission FAILS", 1, alter(GOOD, ridesSkipped=1, ridesSubmitted=2399)),
    case("pings skipped above one percent FAILS", 1, alter(GOOD, pingsSkipped=2000)),
    case("pings skipped below one percent passes and is printed", 0, alter(GOOD, pingsSkipped=100, pingsOk=143900)),
    case("a run that offered a tenth of its rides FAILS", 1, alter(GOOD, ridesSubmitted=240, durableTrips=240, durableDecided=240, durableMatched=240, tripsByShard={"shard-0": {"2": 120}, "shard-1": {"1": 120}}, tripsByStatus={"MATCHED": 240})),
    case("a run whose drivers stopped pinging FAILS", 1, alter(GOOD, pingsOk=4000)),
    case("a summary without ridesPerSecond FAILS rather than skipping the check", 1, {k: v for k, v in GOOD.items() if k != "ridesPerSecond"}),
    case("one ride http error still FAILS", 1, alter(GOOD, rideErrors=1)),
    case("an undecided ride still FAILS", 1, alter(GOOD, durableRequested=5, durableDecided=2395)),
    case("a shard holding two cities still FAILS", 1, alter(GOOD, tripsByShard={"shard-0": {"1": 1200, "2": 1200}})),
]
print()
print("gate cases:", sum(results), "of", len(results), "behaved as specified")
sys.exit(0 if all(results) else 1)
