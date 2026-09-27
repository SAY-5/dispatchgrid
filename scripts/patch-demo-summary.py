#!/usr/bin/env python3
"""Replace the README compose-demo block with the summary of a real run, or refuse.

The block is never written by hand. It is rendered from the two artifacts of one run:
the generator's summary JSON, which holds the numbers and the text rendered from them,
and the demo script's run JSON, which holds the host-side facts the generator cannot
see. A run that did not complete, or a provenance field the demo path never supplied,
refuses instead of publishing a figure nobody measured.
"""

import argparse
import json
from pathlib import Path
import re
import sys


START = "<!-- demo-summary:start -->"
END = "<!-- demo-summary:end -->"
HEADER = "== dispatchgrid load summary =="
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
# Numbers that must appear in the rendered text exactly as the structured fields hold them,
# so an edited text cannot report a figure the artifact does not contain.
CHECKED_NUMBERS = (
    ("ridesSubmitted", "rides submitted     {value},"),
    ("pingsOk", "pings ok={value} "),
    ("durableTrips", "of {value} trip rows,"),
    ("durableMatched", "{value} matched,"),
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


def load(path, where):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise Refused(f"{where}: {error}") from error


def render(summary, run):
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

    body = text_value(summary, "summaryText", "summary")
    if not body.startswith(HEADER):
        raise Refused(f"summary: summaryText does not start with {HEADER!r}")
    for field, shape in CHECKED_NUMBERS:
        value = summary.get(field)
        if not isinstance(value, int):
            raise Refused(f"summary: {field} is not a measured integer")
        if shape.format(value=value) not in body:
            raise Refused(f"summary: the text does not report {field}={value} from the same run")

    header = (
        f"$ {text_value(run, 'command', 'run')}"
        f"    # commit {text_value(run, 'commit', 'run')},"
        f" {text_value(run, 'startedAt', 'run')} -> {text_value(run, 'finishedAt', 'run')},"
        f" {text_value(run, 'machine', 'run')}"
    )
    host = (
        "host load average   "
        f"{text_value(run, 'hostLoadAverageBefore', 'run')} before the run,"
        f" {text_value(run, 'hostLoadAverageAfter', 'run')} after"
        " (one minute average, the kernel hosting the Docker VM)"
    )
    block = "\n".join([START, "```", header, host, body.rstrip("\n"), "```", END])
    lowered = block.lower()
    for placeholder in PLACEHOLDERS:
        if re.search(rf"\b{re.escape(placeholder)}\b", lowered):
            raise Refused(f"the rendered block still contains {placeholder!r}")
    return block + "\n"


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
    try:
        block = render(load(args.summary, "summary"), load(args.run, "run"))
        changed = patch(Path(args.readme), block)
    except Refused as refusal:
        print(f"REFUSED: {refusal}")
        return 1
    print(f"{'patched' if changed else 'unchanged'}: {args.readme}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
