from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from analyze_timing_ablation import WRAP_US, analyze
from combine_timing_ablation import combine


def envelope(wall_ns: int, raw: str) -> dict:
    return {"ingest_wall_time_ns": wall_ns, "raw": raw}


def csi(wall_ns: int, raw_us: int) -> dict:
    raw = f'CSI_DATA,1,aa,-40,1,1,0,0,0,0,0,0,0,0,-95,0,6,0,{raw_us},0,128,0,2,0,"[1,2]"'
    return envelope(wall_ns, raw)


class TimingAblationTests(unittest.TestCase):
    def test_naive_counts_wrap_as_false_reset_and_full_detects_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "run"
            source = run / "sources" / "a"
            source.mkdir(parents=True)
            rows = [
                envelope(1_000_000_000, "CWSLAB_TIMING_HEARTBEAT boot_epoch=1"),
                csi(2_000_000_000, WRAP_US - 100),
                csi(3_000_000_000, 100),
                envelope(4_000_000_000, "CSI_PROFILE boot_epoch=2"),
                csi(4_100_000_000, 50),
            ]
            with gzip.open(source / "chunk.ndjson.gz", "wt", encoding="utf-8") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
            trial = {"fault_type": "esp-reboot", "source_id": "a"}
            ledger_rows = [
                {"event_type": "campaign-start", "wall_time_ns": 3_500_000_000},
                {"event_type": "trial-injected", "trial": trial},
                {"event_type": "campaign-complete", "wall_time_ns": 5_000_000_000},
            ]
            ledger = root / "ledger.ndjson"
            ledger.write_text("".join(json.dumps(row) + "\n" for row in ledger_rows), encoding="utf-8")
            report = analyze(run, ledger)
        self.assertEqual(report["ground_truth_timestamp_wraps"], 1)
        self.assertEqual(report["ground_truth_boot_epoch_transitions"], 1)
        self.assertEqual(report["ground_truth_reset_profiles"], 1)
        self.assertEqual(report["methods"]["naive-reset-on-regression"]["false_positive"], 1)
        self.assertEqual(report["methods"]["failure-aware"]["true_positive"], 1)

    def test_combines_confusion_counts(self) -> None:
        sample = {
            "source_id": "a", "duration_hours": 1, "planned_reboot_trials": 1,
            "ground_truth_reset_profiles": 1, "ground_truth_timestamp_wraps": 1,
            "methods": {
                "m": {
                    "true_positive": 1, "false_positive": 1, "false_negative": 0,
                    "reset_detection_latency": {"n": 1},
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            first = Path(temporary) / "a.json"
            second = Path(temporary) / "b.json"
            first.write_text(json.dumps(sample), encoding="utf-8")
            second.write_text(json.dumps({**sample, "source_id": "b"}), encoding="utf-8")
            report = combine([first, second])
        self.assertEqual(report["methods"]["m"]["true_positive"], 2)
        self.assertEqual(report["methods"]["m"]["false_reset_rate_per_node_hour"], 1)


if __name__ == "__main__":
    unittest.main()
