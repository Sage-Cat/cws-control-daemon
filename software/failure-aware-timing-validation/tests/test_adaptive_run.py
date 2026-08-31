from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from analyze_adaptive_run import decision_stream_diagnostics, jsonl, write_clean_events


class AdaptiveRunTests(unittest.TestCase):
    def test_recovers_prefixed_event_and_records_the_damage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "events.ndjson"
            destination = Path(temporary) / "clean.ndjson"
            source.write_text(
                '{"event_type":"normal","wall_time_ns":1}\n'
                'test{"event_type":"recovered","wall_time_ns":2}\n'
                'not-json\n',
                encoding="utf-8",
            )

            accounting = write_clean_events(source, destination)
            recovered, errors = jsonl(destination)

        self.assertEqual([row["event_type"] for row in recovered], ["normal", "recovered"])
        self.assertEqual(errors, [])
        self.assertEqual(accounting["records"], 2)
        self.assertEqual(accounting["event_types"], {"normal": 1, "recovered": 1})
        self.assertEqual(accounting["malformed_lines"][0]["prefix"], "test")
        self.assertTrue(accounting["malformed_lines"][0]["recovered"])
        self.assertFalse(accounting["malformed_lines"][1]["recovered"])

    def test_counts_controller_restarts_without_wall_time_regression(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "decisions.ndjson"
            rows = [
                {"decision_sequence": 1, "decision_wall_time_ns": 10, "mode": "apply"},
                {"decision_sequence": 2, "decision_wall_time_ns": 20, "mode": "apply"},
                {"decision_sequence": 1, "decision_wall_time_ns": 30, "mode": "recommend"},
            ]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            report = decision_stream_diagnostics(path)

        self.assertEqual(report["inferred_process_segments"], 2)
        self.assertEqual(report["sequence_regressions"], 1)
        self.assertEqual(report["wall_time_regressions"], 0)
        self.assertEqual(report["mode_counts"], {"apply": 2, "recommend": 1})


if __name__ == "__main__":
    unittest.main()
