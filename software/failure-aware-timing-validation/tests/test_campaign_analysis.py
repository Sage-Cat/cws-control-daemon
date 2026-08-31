from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analyze_campaign import analyze, bootstrap_median_ci, metric_summary


def decision(wall_ns: int, available: bool, timing: bool, quorum: int, applied: bool = False) -> dict:
    return {
        "decision_wall_time_ns": wall_ns,
        "fusion": {"quorum": quorum},
        "evidence": {"a": {"available": available, "timing_valid": timing}},
        "actuation": ([{"source_id": "a", "status": "applied"}] if applied else []),
    }


class CampaignAnalysisTests(unittest.TestCase):
    def test_quantifies_one_trial(self) -> None:
        trial = {
            "trial_id": "auto-001",
            "randomized_sequence": 1,
            "source_id": "a",
            "fault_type": "esp-reboot",
            "condition": "apply",
        }
        events = [
            {"event_type": "campaign-start", "wall_time_ns": 1, "trial_count": 1},
            {
                "event_type": "trial-injected", "wall_time_ns": 2, "trial": trial,
                "command": {"ended_wall_time_ns": 2_000_000_000},
            },
            {
                "event_type": "trial-stable", "wall_time_ns": 6_000_000_000, "trial": trial,
                "stability": {"decision_wall_time_ns": 6_000_000_000},
            },
            {"event_type": "campaign-complete", "wall_time_ns": 7_000_000_000},
        ]
        decisions = [
            decision(2_000_000_000, True, True, 1),
            decision(3_000_000_000, False, False, 0, True),
            decision(5_000_000_000, True, True, 1),
            decision(6_000_000_000, True, True, 1),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = root / "ledger.ndjson"
            log = root / "decisions.ndjson"
            ledger.write_text("".join(json.dumps(row) + "\n" for row in events), encoding="utf-8")
            log.write_text("".join(json.dumps(row) + "\n" for row in decisions), encoding="utf-8")
            report = analyze(log, ledger)
        row = report["individual_trials"][0]
        self.assertEqual(row["detection_latency_seconds"], 1.0)
        self.assertEqual(row["timing_recovery_latency_seconds"], 3.0)
        self.assertEqual(row["system_intervention_latency_seconds"], 1.0)

    def test_metric_summary_includes_distribution(self) -> None:
        summary = metric_summary([1.0, 2.0, 3.0, 4.0])
        self.assertEqual(summary["n"], 4)
        self.assertEqual(summary["median"], 2.5)
        self.assertIsNotNone(summary["bootstrap_median_95ci"])

    def test_bootstrap_default_seed_is_portable(self) -> None:
        values = [1.0, 2.0, 3.0, 4.0]
        self.assertEqual(bootstrap_median_ci(values), bootstrap_median_ci(values, seed=1))


if __name__ == "__main__":
    unittest.main()
