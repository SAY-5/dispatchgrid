#!/usr/bin/env python3
"""Judge one kind end to end run and print the rollout evidence block.

Reads the generator's summary JSON and the rollout window measured by scripts/k8s-e2e.sh, prints
the evidence the README quotes, and exits non-zero with the reasons if the run does not prove what
the block claims. Kept in its own file, rather than inline in the script, so that
scripts/rollout_evidence_cases.py can drive it with runs that must fail.

Usage: rollout_evidence.py SUMMARY_JSON ROLLOUT_SECONDS ROLLOUT_FROM_SECONDS ROLLOUT_TO_SECONDS
where the last two are offsets from the moment the generator started submitting.
"""
import json
import sys
s = json.loads(sys.argv[1])
roll = int(sys.argv[2])
roll_from, roll_to = int(sys.argv[3]), int(sys.argv[4])
duration = s["durationSeconds"]
problems = []
# Coverage: the proof is about a rolling update under load, so the replacements have to sit
# inside the load window. The script already failed on this, and the check is repeated here so
# the printed evidence and the verdict cannot disagree.
if roll_to > duration or roll_from < 0:
    problems.append(f"the rolling update ran from +{roll_from}s to +{roll_to}s of a {duration}s load run, so it was not covered throughout")
# Offered load: a run that skipped or never sent its rides and pings can show zero errors while
# proving nothing, so the generator has to have delivered what the job asked for.
rate = s.get("ridesPerSecond")
per_city = s.get("driversPerCity")
if rate:
    expected_rides = rate * duration
    sent_rides = s["ridesSubmitted"] + s.get("ridesSkipped", 0) + s["rideErrors"]
    if sent_rides < 0.9 * expected_rides:
        problems.append(f"offered ride load short: {sent_rides} of about {expected_rides} in {duration}s at {rate}/s")
else:
    problems.append("the summary does not record ridesPerSecond, so the offered load cannot be checked")
expected_pings = s["drivers"] * duration
sent_pings = s["pingsOk"] + s["pingErrors"] + s.get("pingsSkipped", 0)
if sent_pings < 0.9 * expected_pings:
    problems.append(f"offered ping load short: {sent_pings} of about {expected_pings} for {s['drivers']} drivers over {duration}s")
# Skips are the in-flight bound doing its job, which means the target fell behind: a skipped ride
# is a submission the run never made, and skipped pings above a small fraction mean the position
# stream was not kept up. Neither belongs in a run that claims the update was invisible.
if s.get("ridesSkipped", 0) != 0:
    problems.append(f"ride submissions skipped at the in-flight bound: {s['ridesSkipped']}")
if s.get("pingsSkipped", 0) > 0.01 * max(1, s["pingsOk"]):
    problems.append(f"driver pings skipped at the in-flight bound: {s['pingsSkipped']} against {s['pingsOk']} delivered")
if s["rideErrors"] != 0:
    problems.append(f"ride http errors during rollout: {s['rideErrors']}")
if s["pingErrors"] != 0:
    problems.append(f"driver ping http errors during rollout: {s['pingErrors']}")
if s["ridesSubmitted"] == 0:
    problems.append("no rides submitted")
# The counters behind /matching/stats are in process and per pod, so a rolling update resets them
# and one read sees only the pod that answered. Assert decisions from the trip rows in the city
# shards, which survive pod replacement.
if s["durableDecided"] < s["ridesSubmitted"]:
    problems.append(f"undecided rides: {s['ridesSubmitted'] - s['durableDecided']} of {s['ridesSubmitted']}")
if s["durableTrips"] > s["ridesSubmitted"]:
    problems.append(f"trip rows beyond submissions: {s['durableTrips'] - s['ridesSubmitted']} (a retried ride whose first attempt had been stored)")
if s["durableMatched"] < 0.9 * s["ridesSubmitted"]:
    problems.append(f"match rate too low: {s['durableMatched']}/{s['ridesSubmitted']}")
shards = s["tripsByShard"]
for shard, cities in shards.items():
    if len(cities) != 1:
        problems.append(f"{shard} holds trips from cities {sorted(cities)}; expected exactly one city per shard")
print()
print("== rolling update evidence ==")
print(f"rollout duration        {roll}s, from +{roll_from}s to +{roll_to}s of the {duration}s load run")
print(f"load coverage           the whole rolling update ran under load ({roll_to}s of {duration}s used)")
print(f"ride requests           {s['ridesSubmitted']} submitted, {s['rideErrors']} http errors")
print(f"driver position pings   {s['pingsOk']} ok, {s['pingErrors']} http errors")
print(f"driver ping retries     {s.get('pingRetries', 0)} (idempotent upsert retried once on transport failure)")
print(f"ride retries            {s.get('rideRetries', 0)} (retried once on transport failure; a stored first attempt would show as a trip row beyond submissions)")
print(f"sends skipped           {s.get('ridesSkipped', 0)} rides, {s.get('pingsSkipped', 0)} driver pings (in-flight bound reached; skipped and counted, not queued, not http errors)")
print(f"rides decided           {s['durableDecided']} of {s['durableTrips']} trip rows, {s['durableMatched']} matched, {s['durableRequested']} still requested")
print(f"matching counters       {s['matched']} matched / {s['unmatched']} unmatched / {s.get('matchRetries', 0)} retried / {s.get('dropped', 0)} dropped (in process, per pod, reset by the rolling update)")
print(f"matches per minute      {s['matchesPerMinuteRun']} (run), {s['matchesPerMinuteWindow']} (trailing window, answering pod only)")
print(f"match latency           p50={s['p50LatencyMs']}ms p95={s['p95LatencyMs']}ms p99={s['p99LatencyMs']}ms (answering pod reservoir)")
print(f"trips by shard          {json.dumps(shards)}")
if problems:
    print("RESULT: FAIL")
    for p in problems:
        print(" - " + p)
    sys.exit(1)
print("RESULT: PASS (zero request errors, full offered load, every replacement under load)")
