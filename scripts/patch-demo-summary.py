#!/usr/bin/env python3
"""Replace the README compose-demo block with the summary of a real run, or refuse.

The block is never written by hand. It is rendered from the two artifacts of one run:
the generator's summary JSON, which holds the numbers and the text rendered from them,
and the demo script's run JSON, which holds the host-side facts the generator cannot
see. The fenced part of the block is that text exactly as the generator printed it,
and the line above the fence carries the host-side facts. Nothing is written unless
the run completed at the commit checked out here on an unmodified tree, the two
artifacts name the same commit and machine, the measured window lies inside the span
the run record gives, every line of the text is what the fields stored beside it
render to, and no field is missing or a placeholder.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys


START = "<!-- demo-summary:start -->"
END = "<!-- demo-summary:end -->"
# The summary as io.dispatchgrid.loadgen.LoadGen.renderSummary prints it. Every figure is a field
# stored beside the text, named here in braces; the words around them are fixed. A text that is
# not exactly these lines filled in from its own fields was cut short or edited after the run.
FORMAT = (
    "== dispatchgrid load summary ==",
    "run                 {durationSeconds} s at {ridesPerSecond} rides/s, cities {cities}",
    "commit              {commit}",
    "measured window     {startedAt} -> {finishedAt}",
    "machine             {machine}; container {kernel}",
    "load average        {loadAverageAtStart} at the start of the load, {loadAverageAtEnd} at the end (kernel above)",
    "drivers             {drivers} ({driversPerCity} per city), pings ok={pingsOk} errors={pingErrors} retries={pingRetries} skipped={pingsSkipped}",
    "rides submitted     {ridesSubmitted}, http errors={rideErrors}, retries={rideRetries}, skipped={ridesSkipped}, by shard {submittedByShard}",
    "in-flight bound     {maxInFlightPings} pings, {maxInFlightRides} rides; a send past the bound is skipped and counted, not queued, and is not an http error",
    "decided (durable)   {durableDecided} of {durableTrips} trip rows, {durableMatched} matched, {durableRequested} still requested",
    "matching counters   matched={matched} unmatched={unmatched} retried={matchRetries} dropped={dropped} (in process, per pod, reset by a rollout)",
    "matches per minute  {matchesPerMinuteRun} over the {runSeconds} s run (matching-service trailing 60 s window: {matchesPerMinuteWindow})",
    "match latency       p50={p50LatencyMs} ms  p95={p95LatencyMs} ms  p99={p99LatencyMs} ms",
    "shard distribution  {tripsByShard}",
)
COUNT_FIELDS = (
    "durationSeconds", "ridesPerSecond", "runSeconds", "drivers", "driversPerCity",
    "pingsOk", "pingErrors", "pingRetries", "pingsSkipped", "maxInFlightPings",
    "ridesSubmitted", "rideErrors", "rideRetries", "ridesSkipped", "maxInFlightRides",
    "durableDecided", "durableTrips", "durableMatched", "durableRequested",
    "matched", "unmatched", "matchRetries", "dropped",
    "matchesPerMinuteRun", "matchesPerMinuteWindow",
    "p50LatencyMs", "p95LatencyMs", "p99LatencyMs",
)
# "unspecified" is what the generator records for a missing RUN_COMMIT or RUN_MACHINE, and
# "unavailable" what it records for a load average the platform would not give it.
PLACEHOLDERS = ("unspecified", "unavailable", "unknown", "placeholder", "todo", "tbd", "n/a")
RUN_FIELDS = (
    "command",
    "commit",
    "machine",
    "startedAt",
    "finishedAt",
    "hostLoadAverageBefore",
    "hostLoadAverageAfter",
    "stackMemory",
)
PROVENANCE_FIELDS = (
    "commit",
    "machine",
    "kernel",
    "startedAt",
    "finishedAt",
    "loadAverageAtStart",
    "loadAverageAtEnd",
)


class Refused(Exception):
    """The run does not support a figure, so nothing is written."""


def text_value(record, field, where):
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise Refused(f"{where}: {field} is missing or empty")
    if value.strip().lower() in PLACEHOLDERS:
        raise Refused(f"{where}: {field} is the placeholder {value.strip()!r}, so nothing was measured")
    return value.strip()


def utc_time(record, field, where):
    """A timestamp in the one form both artifacts write, UTC to the second."""
    value = text_value(record, field, where)
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise Refused(f"{where}: {field} {value!r} is not a UTC time to the second") from error


def count(value, field):
    if type(value) is not int:
        raise Refused(f"summary: {field} is not a measured integer")
    return value


def load(path, where):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise Refused(f"{where}: {error}") from error


def head_commit(repo):
    """The full object name of the commit checked out in the repository the README is in."""
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", "HEAD"],
            text=True, capture_output=True, check=False,
        )
    except OSError as error:
        raise Refused(f"{repo}: cannot run git to read HEAD ({error})") from error
    if result.returncode != 0:
        raise Refused(f"{repo}: cannot read HEAD ({result.stderr.strip()})")
    return result.stdout.strip()


def rendered_text(summary):
    """The text the stored fields render to, the way the generator renders it from them."""
    values = {field: count(summary.get(field), field) for field in COUNT_FIELDS}
    provenance = summary["provenance"]
    for field in PROVENANCE_FIELDS:
        values[field] = provenance[field]

    ids, names = summary.get("cityIds"), summary.get("cities")
    if (not isinstance(ids, list) or not isinstance(names, list) or len(ids) != len(names)
            or not all(isinstance(name, str) for name in names)):
        raise Refused("summary: cityIds and cities are not one list of cities")
    values["cities"] = ", ".join(f"{count(i, 'cityIds')}={name}" for i, name in zip(ids, names))

    # The generator keeps both maps sorted, by shard name and then by city id.
    submitted = summary.get("submittedByShard")
    if not isinstance(submitted, dict):
        raise Refused("summary: submittedByShard is not a map of shards")
    values["submittedByShard"] = "{" + ", ".join(
        f"{shard}={count(n, 'submittedByShard')}" for shard, n in sorted(submitted.items())) + "}"
    shards = summary.get("tripsByShard")
    if not isinstance(shards, dict) or not all(isinstance(c, dict) for c in shards.values()):
        raise Refused("summary: tripsByShard is not a map of shards to cities")
    try:
        values["tripsByShard"] = " | ".join(
            f"{shard}: " + ", ".join(
                f"city {city} -> {count(n, 'tripsByShard')} trips"
                for city, n in sorted(cities.items(), key=lambda item: int(item[0])))
            for shard, cities in sorted(shards.items()))
    except ValueError as error:
        raise Refused(f"summary: tripsByShard has a city that is not an id ({error})") from error
    return "".join(line.format(**values) + "\n" for line in FORMAT)


def summary_text(summary):
    """The text as the generator printed it, refused unless its own fields render it exactly."""
    text = summary.get("summaryText")
    if not isinstance(text, str):
        raise Refused("summary: summaryText is missing")
    expected = rendered_text(summary)
    if text == expected:
        return text
    found, wanted = text.split("\n"), expected.split("\n")
    for number, (line, want) in enumerate(zip(found, wanted), start=1):
        if line != want:
            raise Refused(
                f"summary: line {number} of summaryText is not what the fields stored beside it"
                f" render to: it reads {line!r} where the fields give {want!r}"
            )
    raise Refused("summary: summaryText does not end where the text its fields render to ends")


def render(summary, run, head):
    if summary.get("complete") is not True:
        raise Refused("summary: the run did not complete, so it carries no summary to quote")
    exit_code = run.get("loadgenExitCode")
    if exit_code != 0:
        raise Refused(f"run: the load generator exited {exit_code!r}")
    for field in RUN_FIELDS:
        text_value(run, field, "run")
    provenance = summary.get("provenance")
    if not isinstance(provenance, dict):
        raise Refused("summary: no provenance recorded")
    for field in PROVENANCE_FIELDS:
        text_value(provenance, field, "provenance")
    commit = text_value(provenance, "commit", "provenance")
    if commit != text_value(run, "commit", "run"):
        raise Refused("the two artifacts disagree about the commit, so they are not one run")
    if commit.endswith("-dirty"):
        raise Refused(f"the run was made on a modified tree ({commit}), so it is not reproducible")
    if not re.fullmatch(r"[0-9a-f]{7,40}", commit):
        raise Refused(f"the run's commit {commit!r} is not an abbreviated object name")
    # A failed attempt keeps the last completed pair, so agreeing artifacts can still be an
    # earlier run's. Only a run of the commit checked out here describes this README's code.
    if not head.startswith(commit):
        raise Refused(
            f"the run was made at commit {commit} but HEAD is {head[:len(commit)]}, so these"
            " artifacts are from an earlier run; run make demo at this commit before patching"
        )
    # The demo script passes one RUN_MACHINE to the generator and writes the same text into its own
    # record, and it records its span around the whole run, so the measured load lies inside it.
    if text_value(provenance, "machine", "provenance") != text_value(run, "machine", "run"):
        raise Refused("the two artifacts disagree about the machine, so they are not one run")
    window = [utc_time(provenance, field, "provenance") for field in ("startedAt", "finishedAt")]
    span = [utc_time(run, field, "run") for field in ("startedAt", "finishedAt")]
    if not span[0] <= window[0] <= window[1] <= span[1]:
        raise Refused(
            f"the measured window {provenance['startedAt']} -> {provenance['finishedAt']} does not"
            f" lie inside the run record's span {run['startedAt']} -> {run['finishedAt']},"
            " so the two artifacts are not one run"
        )

    body = summary_text(summary)
    caption = (
        f"`{text_value(run, 'command', 'run')}` at commit {commit},"
        f" {text_value(run, 'startedAt', 'run')} to {text_value(run, 'finishedAt', 'run')};"
        f" host load average {text_value(run, 'hostLoadAverageBefore', 'run')} before the run"
        f" and {text_value(run, 'hostLoadAverageAfter', 'run')} after (one minute averages on the"
        f" machine that launched it); {text_value(run, 'stackMemory', 'run')} in use halfway"
        " through the load, as `docker stats --no-stream` reported it:"
    )
    block = f"{START}\n{caption}\n\n```\n{body}```\n{END}\n"
    lowered = block.lower()
    for placeholder in PLACEHOLDERS:
        if re.search(rf"\b{re.escape(placeholder)}\b", lowered):
            raise Refused(f"the rendered block still contains {placeholder!r}")
    return block


def patch(readme, block):
    current = readme.read_text()
    pattern = re.compile(re.escape(START) + ".*?" + re.escape(END) + r"\n", re.DOTALL)
    if not pattern.search(current):
        raise Refused(f"{readme}: no {START} ... {END} block to replace")
    patched = pattern.sub(lambda _: block, current, count=1)
    if patched == current:
        return False
    readme.write_text(patched)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", help="loadgen-summary.json written by the run")
    parser.add_argument("run", help="demo-run.json written by scripts/compose-demo.sh")
    parser.add_argument("--readme", default="README.md")
    args = parser.parse_args()
    readme = Path(args.readme)
    try:
        head = head_commit(readme.resolve().parent)
        block = render(load(args.summary, "summary"), load(args.run, "run"), head)
        changed = patch(readme, block)
    except Refused as refusal:
        print(f"REFUSED: {refusal}")
        return 1
    print(f"{'patched' if changed else 'unchanged'}: {args.readme}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
