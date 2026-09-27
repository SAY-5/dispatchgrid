"""Behavioral regressions for the README compose-demo patcher (no stack needed)."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
PATCHER = ROOT / "scripts/patch-demo-summary.py"
START = "<!-- demo-summary:start -->"
END = "<!-- demo-summary:end -->"

SUMMARY_TEXT = """== dispatchgrid load summary ==
run                 60 s at 10 rides/s, cities 1=alpha, 2=beta
commit              7f3a91c
measured window     2026-09-27T05:10:33Z -> 2026-09-27T05:11:33Z
machine             Darwin arm64 10 CPU, Docker VM 6 CPU / 8307380224 B; container linux/aarch64, 6 CPU, JDK 21.0.8
load average        1.42 at the start of the load, 3.08 at the end (kernel above)
drivers             600 (300 per city), pings ok=37200 errors=0 retries=0 skipped=0
rides submitted     603, http errors=0, retries=0, skipped=0, by shard {shard-0=301, shard-1=302}
decided (durable)   603 of 603 trip rows, 603 matched, 0 still requested
shard distribution  shard-0: city 2 -> 301 trips | shard-1: city 1 -> 302 trips
"""


def summary():
    return {
        "provenance": {
            "commit": "7f3a91c",
            "machine": "Darwin arm64 10 CPU, Docker VM 6 CPU / 8307380224 B",
            "kernel": "linux/aarch64, 6 CPU, JDK 21.0.8",
            "startedAt": "2026-09-27T05:10:33Z",
            "finishedAt": "2026-09-27T05:11:33Z",
            "loadAverageAtStart": "1.42",
            "loadAverageAtEnd": "3.08",
        },
        "ridesSubmitted": 603, "pingsOk": 37200,
        "durableTrips": 603, "durableMatched": 603,
        "complete": True, "summaryText": SUMMARY_TEXT,
    }


def run_record():
    return {
        "command": "make demo",
        "commit": "7f3a91c",
        "machine": "Darwin arm64 10 CPU, Docker VM 6 CPU / 8307380224 B",
        "startedAt": "2026-09-27T05:09:40Z",
        "finishedAt": "2026-09-27T05:12:14Z",
        "hostLoadAverageBefore": "9.12",
        "hostLoadAverageAfter": "8.44",
        "loadgenExitCode": 0,
    }


class DemoSummaryPatcherTest(unittest.TestCase):
    def patch(self, summary_json, run_json, readme=f"before\n{START}\n```\nold\n```\n{END}\nafter\n"):
        with tempfile.TemporaryDirectory() as temp:
            paths = {}
            for name, value in (("summary", summary_json), ("run", run_json)):
                paths[name] = Path(temp) / f"{name}.json"
                paths[name].write_text(value if isinstance(value, str) else json.dumps(value))
            readme_path = Path(temp) / "README.md"
            readme_path.write_text(readme)
            result = subprocess.run(
                [sys.executable, str(PATCHER), str(paths["summary"]), str(paths["run"]),
                 "--readme", str(readme_path)],
                text=True, capture_output=True, check=False,
            )
            return result, readme_path.read_text()

    def assert_refused(self, summary_json, run_json, readme=None):
        original = f"before\n{START}\n```\nold\n```\n{END}\nafter\n" if readme is None else readme
        result, text = self.patch(summary_json, run_json, original)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("REFUSED", result.stdout)
        self.assertEqual(text, original, "a refused patch must leave the README alone")

    def test_patches_the_block_from_a_complete_run(self):
        result, text = self.patch(summary(), run_record())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        block = text.split(START)[1].split(END)[0]
        self.assertIn("$ make demo    # commit 7f3a91c", block)
        self.assertIn("9.12 before the run, 8.44 after", block)
        self.assertIn("load average        1.42 at the start of the load", block)
        self.assertIn("rides submitted     603,", block)
        self.assertNotIn("old", block)
        self.assertTrue(text.startswith("before\n"))
        self.assertTrue(text.endswith("after\n"))

    def test_refuses_a_run_that_did_not_complete(self):
        incomplete = summary()
        del incomplete["complete"]
        self.assert_refused(incomplete, run_record())

    def test_refuses_a_failed_load_generator(self):
        failed = run_record()
        failed["loadgenExitCode"] = 2
        self.assert_refused(summary(), failed)

    def test_refuses_an_unsupplied_commit_or_machine(self):
        for field in ("commit", "machine"):
            with self.subTest(field=field):
                unspecified = summary()
                unspecified["provenance"][field] = "unspecified"
                self.assert_refused(unspecified, run_record())

    def test_refuses_a_load_average_the_platform_would_not_give(self):
        missing = summary()
        missing["provenance"]["loadAverageAtEnd"] = "unavailable"
        self.assert_refused(missing, run_record())

    def test_refuses_a_missing_host_load_average(self):
        missing = run_record()
        del missing["hostLoadAverageAfter"]
        self.assert_refused(summary(), missing)

    def test_refuses_two_artifacts_from_different_commits(self):
        other = run_record()
        other["commit"] = "0000000"
        self.assert_refused(summary(), other)

    def test_refuses_text_that_reports_a_number_the_run_did_not_measure(self):
        edited = summary()
        edited["summaryText"] = SUMMARY_TEXT.replace("rides submitted     603,", "rides submitted     900,")
        self.assert_refused(edited, run_record())

    def test_refuses_a_run_from_a_modified_tree(self):
        dirty_summary, dirty_run = summary(), run_record()
        dirty_summary["provenance"]["commit"] = "7f3a91c-dirty"
        dirty_summary["summaryText"] = SUMMARY_TEXT.replace("7f3a91c", "7f3a91c-dirty")
        dirty_run["commit"] = "7f3a91c-dirty"
        self.assert_refused(dirty_summary, dirty_run)

    def test_refuses_a_readme_without_the_markers(self):
        self.assert_refused(summary(), run_record(), readme="no markers here\n")

    def test_refuses_unreadable_artifacts(self):
        self.assert_refused("{not json", run_record())


if __name__ == "__main__":
    unittest.main()
