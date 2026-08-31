from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_campaign import (
    Campaign,
    HELPER_ROOT,
    REPOSITORY_ROOT,
    external_results_directory,
    last_json_record,
    render,
)


class CampaignTests(unittest.TestCase):
    def test_render_preserves_argv_boundaries(self) -> None:
        self.assertEqual(
            render(["tool", "--note", "{note}", "{hz}"], note="a value", hz=20),
            ["tool", "--note", "a value", "20"],
        )

    def test_seeded_trials_cover_each_class_and_node(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = {
                "schema_version": 1,
                "seed": 123,
                "safe_rate_hz": 40,
                "trial_classes": [
                    {
                        "fault_type": "esp-reboot",
                        "condition": "apply",
                        "repetitions_per_node": 2,
                        "fault_seconds": 0,
                        "detection_timeout_seconds": 1,
                        "stable_seconds": 1,
                    }
                ],
                "nodes": {"a": {}, "b": {}},
            }
            paths = {
                "ledger": root / "ledger.ndjson",
                "decisions": root / "decisions.ndjson",
                "actuation_inhibit_file": root / "inhibit",
            }
            first = Campaign(config, **paths).build_trials()
            second = Campaign(config, **paths).build_trials()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 4)
        self.assertEqual({trial["source_id"] for trial in first}, {"a", "b"})

    def test_last_json_record_tolerates_partial_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "values.ndjson"
            path.write_bytes(b'{"sequence":1}\n{"sequence":2}\n{"partial":')
            self.assertEqual(last_json_record(path), {"sequence": 2})

    def test_external_destination_requires_run_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary) / "external-run"
            run.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "run.toml"):
                external_results_directory(run)
            (run / "run.toml").write_text("external_run = true\n", encoding="utf-8")
            destination = external_results_directory(run)
            self.assertEqual(
                destination.relative_to(run),
                Path("results/failure_aware_timing_validation"),
            )

    def test_external_destination_rejects_repository_tree(self) -> None:
        synthetic = HELPER_ROOT / "tests" / "synthetic-run"
        synthetic.mkdir(parents=True, exist_ok=True)
        try:
            (synthetic / "run.toml").write_text("schema_version = 2\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "outside the repository"):
                external_results_directory(synthetic)
        finally:
            (synthetic / "run.toml").unlink(missing_ok=True)
            synthetic.rmdir()

    def test_external_destination_rejects_repository_root(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the repository"):
            external_results_directory(REPOSITORY_ROOT)


if __name__ == "__main__":
    unittest.main()
