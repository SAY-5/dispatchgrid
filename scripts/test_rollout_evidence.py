"""Behavioral regressions for the kind load/rollout evidence gate (no cluster needed)."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


def healthy_summary():
    # Hand-checked: 120 s, 10 rides/s and 600 pings/s, plus 1,200 seed pings.
    return {
        "durationSeconds": 120, "ridesPerSecond": 10, "drivers": 600,
        "loadStartedAtEpochMs": 1000000, "loadStoppedAtEpochMs": 1120000,
        "loadSamples": [
            {"epochMs": 1000000 + second * 1000,
             "ridesSubmitted": second * 10, "pingsOk": 1200 + second * 600}
            for second in range(121)
        ],
        "rideErrors": 0, "pingErrors": 0, "ridesSkipped": 0, "pingsSkipped": 0,
        "ridesSubmitted": 1200, "pingsOk": 73200,
        "durableDecided": 1200, "durableTrips": 1200, "durableMatched": 1200,
        "durableRequested": 0, "matched": 600, "unmatched": 0,
        "matchesPerMinuteRun": 600, "matchesPerMinuteWindow": 600,
        "p50LatencyMs": 10, "p95LatencyMs": 100, "p99LatencyMs": 200,
        "tripsByShard": {"shard-0": {"2": 600}, "shard-1": {"1": 600}},
    }


def rollouts():
    return [
        {"service": "rider-request-service", "startEpochMs": 1010000, "endEpochMs": 1040000},
        {"service": "driver-location-service", "startEpochMs": 1040000, "endEpochMs": 1070000},
        {"service": "matching-service", "startEpochMs": 1070000, "endEpochMs": 1100000},
    ]


class RolloutEvidenceTest(unittest.TestCase):
    def check_evidence(self, summary, windows=None):
        windows = rollouts() if windows is None else windows
        checker = ROOT / "scripts/verify-rollout-evidence.py"
        with tempfile.TemporaryDirectory() as temp:
            summary_path = Path(temp) / "summary.json"
            windows_path = Path(temp) / "windows.jsonl"
            summary_path.write_text(json.dumps(summary))
            windows_path.write_text("".join(json.dumps(w) + "\n" for w in windows))
            return subprocess.run(
                [sys.executable, str(checker), str(summary_path), str(windows_path)],
                text=True, capture_output=True, check=False,
            )

    def assert_rejected(self, summary, windows=None):
        result = self.check_evidence(summary, windows)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("RESULT: FAIL", result.stdout)

    def test_rejects_load_that_finishes_before_matching_rollout(self):
        summary = healthy_summary()
        summary["loadStoppedAtEpochMs"] = 1060000
        summary["loadSamples"] = summary["loadSamples"][:61]
        self.assert_rejected(summary)

    def test_rejects_one_skipped_ride_submission(self):
        summary = healthy_summary()
        summary["ridesSkipped"] = 1
        self.assert_rejected(summary)

    def test_rejects_pings_skipped_beyond_the_bound(self):
        summary = healthy_summary()
        summary["pingsSkipped"] = int(summary["pingsOk"] * 0.02)
        self.assert_rejected(summary)

    def test_accepts_a_few_skipped_pings_and_reports_them(self):
        summary = healthy_summary()
        summary["pingsSkipped"] = int(summary["pingsOk"] * 0.005)
        result = self.check_evidence(summary)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"{summary['pingsSkipped']} skipped", result.stdout)

    def test_rejects_final_request_errors(self):
        for field in ("rideErrors", "pingErrors"):
            with self.subTest(field=field):
                summary = healthy_summary()
                summary[field] = 1
                self.assert_rejected(summary)

    def test_rejects_missing_measured_load_window(self):
        summary = healthy_summary()
        del summary["loadStartedAtEpochMs"]
        self.assert_rejected(summary)

    def test_rejects_missing_service_rollout(self):
        self.assert_rejected(healthy_summary(), rollouts()[:2])

    def test_rejects_stalled_successful_traffic_during_rollout(self):
        for field in ("ridesSubmitted", "pingsOk"):
            with self.subTest(field=field):
                summary = healthy_summary()
                stalled = summary["loadSamples"][40][field]
                for sample in summary["loadSamples"][40:51]:
                    sample[field] = stalled
                self.assert_rejected(summary)

    def test_rejects_undecided_or_duplicate_durable_trips(self):
        for field, count in (("durableDecided", 1199), ("durableTrips", 1201)):
            with self.subTest(field=field):
                summary = healthy_summary()
                summary[field] = count
                self.assert_rejected(summary)

    def test_rejects_old_summary_without_skip_telemetry(self):
        summary = healthy_summary()
        del summary["pingsSkipped"]
        self.assert_rejected(summary)

    def test_rejects_large_unobserved_gap(self):
        summary = healthy_summary()
        del summary["loadSamples"][40:50]
        self.assert_rejected(summary)

    def test_rejects_insufficient_measured_rate(self):
        summary = healthy_summary()
        for sample in summary["loadSamples"]:
            sample["ridesSubmitted"] //= 10
        summary["ridesSubmitted"] = 120
        summary["durableDecided"] = 120
        summary["durableTrips"] = 120
        summary["durableMatched"] = 120
        self.assert_rejected(summary)

    def test_accepts_all_replacements_inside_sustained_load(self):
        result = self.check_evidence(healthy_summary())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("RESULT: PASS", result.stdout)

    def test_rejects_non_monotonic_sample_clock(self):
        summary = healthy_summary()
        summary["loadSamples"][50]["epochMs"] = 1048000
        self.assert_rejected(summary)

    def test_rejects_clock_boundary_without_margin(self):
        windows = rollouts()
        windows[-1]["endEpochMs"] = 1120000
        self.assert_rejected(healthy_summary(), windows)


if __name__ == "__main__":
    unittest.main()
