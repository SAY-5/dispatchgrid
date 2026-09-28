"""Behavioral regressions for the README compose-demo patcher (no stack needed)."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
PATCHER = ROOT / "scripts/patch-demo-summary.py"
START = "<!-- demo-summary:start -->"
END = "<!-- demo-summary:end -->"
README = f"before\n{START}\n```\nold\n```\n{END}\nafter\n"
MACHINE = "Darwin arm64 10 CPU, Docker VM 6 CPU / 7.8 GiB"
# The scratch repositories must not pick up the signing or hook settings of whoever runs this.
GIT_ENV = dict(
    os.environ,
    GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
    GIT_AUTHOR_NAME="demo", GIT_AUTHOR_EMAIL="demo@example.com",
    GIT_COMMITTER_NAME="demo", GIT_COMMITTER_EMAIL="demo@example.com",
)


def summary_text(commit):
    return f"""== dispatchgrid load summary ==
run                 60 s at 10 rides/s, cities 1=austin, 2=seattle
commit              {commit}
measured window     2026-09-27T05:10:33Z -> 2026-09-27T05:11:33Z
machine             {MACHINE}; container linux/aarch64, 6 CPU, JDK 21.0.8
load average        1.42 at the start of the load, 3.08 at the end (kernel above)
drivers             600 (300 per city), pings ok=37200 errors=0 retries=0 skipped=0
rides submitted     603, http errors=0, retries=0, skipped=0, by shard {{shard-0=301, shard-1=302}}
in-flight bound     1200 pings, 100 rides; a send past the bound is skipped and counted, not queued, and is not an http error
decided (durable)   603 of 603 trip rows, 603 matched, 0 still requested
matching counters   matched=603 unmatched=0 retried=0 dropped=0 (in process, per pod, reset by a rollout)
matches per minute  603 over the 60 s run (matching-service trailing 60 s window: 603)
match latency       p50=14 ms  p95=53 ms  p99=271 ms
shard distribution  shard-0: city 2 -> 301 trips | shard-1: city 1 -> 302 trips
"""


class DemoSummaryPatcherTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.git("init", "-q")
        self.commit = self.commit_now("the change the demo ran")

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.repo), *args],
            env=GIT_ENV, text=True, capture_output=True, check=True,
        ).stdout.strip()

    def commit_now(self, message):
        self.git("commit", "-q", "--allow-empty", "-m", message)
        return self.git("rev-parse", "--short", "HEAD")

    def summary(self):
        return {
            "provenance": {
                "commit": self.commit,
                "machine": MACHINE,
                "kernel": "linux/aarch64, 6 CPU, JDK 21.0.8",
                "startedAt": "2026-09-27T05:10:33Z",
                "finishedAt": "2026-09-27T05:11:33Z",
                "loadAverageAtStart": "1.42",
                "loadAverageAtEnd": "3.08",
            },
            "durationSeconds": 60, "ridesPerSecond": 10, "runSeconds": 60,
            "cities": ["austin", "seattle"], "cityIds": [1, 2],
            "drivers": 600, "driversPerCity": 300,
            "pingsOk": 37200, "pingErrors": 0, "pingRetries": 0, "pingsSkipped": 0,
            "maxInFlightPings": 1200,
            "ridesSubmitted": 603, "rideErrors": 0, "rideRetries": 0, "ridesSkipped": 0,
            "maxInFlightRides": 100,
            "matched": 603, "unmatched": 0, "matchRetries": 0, "dropped": 0,
            "matchesPerMinuteRun": 603, "matchesPerMinuteWindow": 603,
            "p50LatencyMs": 14, "p95LatencyMs": 53, "p99LatencyMs": 271,
            "submittedByShard": {"shard-0": 301, "shard-1": 302},
            "tripsByShard": {"shard-0": {"2": 301}, "shard-1": {"1": 302}},
            "durableTrips": 603, "durableRequested": 0, "durableDecided": 603,
            "durableMatched": 603,
            "summaryText": summary_text(self.commit), "complete": True,
        }

    def run_record(self):
        return {
            "command": "make demo",
            "commit": self.commit,
            "machine": MACHINE,
            "startedAt": "2026-09-27T05:09:40Z",
            "finishedAt": "2026-09-27T05:12:14Z",
            "hostLoadAverageBefore": "9.12",
            "hostLoadAverageAfter": "8.44",
            "stackMemory": "1849 MiB across 9 containers",
            "loadgenExitCode": 0,
        }

    def patch(self, summary_json, run_json, readme=README):
        paths = {}
        for name, value in (("summary", summary_json), ("run", run_json)):
            paths[name] = self.repo / f"{name}.json"
            paths[name].write_text(value if isinstance(value, str) else json.dumps(value))
        readme_path = self.repo / "README.md"
        readme_path.write_text(readme)
        result = subprocess.run(
            [sys.executable, str(PATCHER), str(paths["summary"]), str(paths["run"]),
             "--readme", str(readme_path)],
            text=True, capture_output=True, check=False,
        )
        return result, readme_path.read_text()

    def assert_refused(self, summary_json, run_json, readme=README):
        result, text = self.patch(summary_json, run_json, readme)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("REFUSED", result.stdout)
        self.assertEqual(text, readme, "a refused patch must leave the README alone")
        return result

    def test_publishes_the_whole_summary_text_verbatim_under_the_run_caption(self):
        result, text = self.patch(self.summary(), self.run_record())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        block = text.split(START + "\n")[1].split(END)[0]
        caption, fenced = block.split("\n\n```\n")
        self.assertEqual(fenced, summary_text(self.commit) + "```\n")
        self.assertIn(f"`make demo` at commit {self.commit},", caption)
        self.assertIn("host load average 9.12 before the run and 8.44 after", caption)
        self.assertIn("1849 MiB across 9 containers in use halfway through the load", caption)
        self.assertTrue(text.startswith("before\n"))
        self.assertTrue(text.endswith(f"{END}\nafter\n"))

    def test_refuses_a_summary_text_missing_a_line_the_generator_prints(self):
        cut = self.summary()
        lines = cut["summaryText"].splitlines(keepends=True)
        del lines[1]
        cut["summaryText"] = "".join(lines)
        self.assert_refused(cut, self.run_record())

    def test_refuses_a_summary_text_with_a_line_cut_short(self):
        cut = self.summary()
        lines = cut["summaryText"].splitlines(keepends=True)
        lines[-1] = lines[-1][:40] + "\n"
        cut["summaryText"] = "".join(lines)
        result = self.assert_refused(cut, self.run_record())
        self.assertIn("line 14 of summaryText", result.stdout)

    def test_refuses_a_summary_without_a_field_its_text_prints(self):
        older = self.summary()
        del older["runSeconds"]
        result = self.assert_refused(older, self.run_record())
        self.assertIn("runSeconds", result.stdout)

    def test_refuses_a_run_made_at_an_earlier_commit_than_head(self):
        summary, run = self.summary(), self.run_record()
        later = self.commit_now("a change made after the demo ran")
        result = self.assert_refused(summary, run)
        self.assertIn(f"made at commit {self.commit} but HEAD is {later}", result.stdout)

    def test_refuses_a_run_that_did_not_complete(self):
        incomplete = self.summary()
        del incomplete["complete"]
        self.assert_refused(incomplete, self.run_record())

    def test_refuses_a_failed_load_generator(self):
        failed = self.run_record()
        failed["loadgenExitCode"] = 2
        self.assert_refused(self.summary(), failed)

    def test_refuses_an_unsupplied_commit_or_machine(self):
        for field in ("commit", "machine"):
            with self.subTest(field=field):
                unspecified = self.summary()
                unspecified["provenance"][field] = "unspecified"
                self.assert_refused(unspecified, self.run_record())

    def test_refuses_a_load_average_the_platform_would_not_give(self):
        missing = self.summary()
        missing["provenance"]["loadAverageAtEnd"] = "unavailable"
        self.assert_refused(missing, self.run_record())

    def test_refuses_a_missing_host_load_average(self):
        missing = self.run_record()
        del missing["hostLoadAverageAfter"]
        self.assert_refused(self.summary(), missing)

    def test_refuses_two_artifacts_from_different_commits(self):
        other = self.run_record()
        other["commit"] = "0000000"
        self.assert_refused(self.summary(), other)

    def test_refuses_two_artifacts_from_different_machines(self):
        other = self.run_record()
        other["machine"] = "Linux x86_64 4 CPU, Docker VM 4 CPU / 15.6 GiB"
        result = self.assert_refused(self.summary(), other)
        self.assertIn("disagree about the machine", result.stdout)

    def test_refuses_a_measured_window_outside_the_run_record_span(self):
        # The fixture's load ran from 05:10:33 to 05:11:33 on 2026-09-27.
        for started, finished in (
            ("2026-09-27T05:10:34Z", "2026-09-27T05:12:14Z"),
            ("2026-09-27T05:09:40Z", "2026-09-27T05:11:32Z"),
            ("2026-09-28T05:09:40Z", "2026-09-28T05:12:14Z"),
        ):
            with self.subTest(started=started, finished=finished):
                other = self.run_record()
                other["startedAt"], other["finishedAt"] = started, finished
                result = self.assert_refused(self.summary(), other)
                self.assertIn("does not lie inside the run record's span", result.stdout)

    def test_refuses_text_that_reports_a_number_its_fields_do_not_hold(self):
        for original, edited in (
            ("rides submitted     603,", "rides submitted     900,"),
            ("p99=271 ms", "p99=71 ms"),
            ("1200 pings, 100 rides", "1000 pings, 100 rides"),
            ("over the 60 s run", "over the 30 s run"),
            ("city 1 -> 302 trips", "city 1 -> 320 trips"),
        ):
            with self.subTest(edited=edited):
                summary = self.summary()
                summary["summaryText"] = summary["summaryText"].replace(original, edited)
                self.assertNotEqual(summary["summaryText"], summary_text(self.commit))
                result = self.assert_refused(summary, self.run_record())
                self.assertIn("is not what the fields stored beside it render to", result.stdout)

    def test_refuses_a_run_from_a_modified_tree(self):
        dirty_summary, dirty_run = self.summary(), self.run_record()
        dirty = f"{self.commit}-dirty"
        dirty_summary["provenance"]["commit"] = dirty
        dirty_summary["summaryText"] = summary_text(dirty)
        dirty_run["commit"] = dirty
        self.assert_refused(dirty_summary, dirty_run)

    def test_refuses_a_readme_without_the_markers(self):
        self.assert_refused(self.summary(), self.run_record(), readme="no markers here\n")

    def test_refuses_unreadable_artifacts(self):
        self.assert_refused("{not json", self.run_record())


if __name__ == "__main__":
    unittest.main()
