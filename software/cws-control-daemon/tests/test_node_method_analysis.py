from __future__ import annotations

import gzip
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "analyze-node-method-blocks.py"
SPEC = importlib.util.spec_from_file_location("node_method_analysis", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class NodeMethodAnalysisTests(unittest.TestCase):
    def test_counts_valid_frames_inside_marker_interval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary) / "run"
            source = run / "sources" / "esp32-s3-a"
            source.mkdir(parents=True)
            events = [
                {"event_type": "method-block-01-static-40-start", "wall_time_ns": 1_000_000_000},
                {"event_type": "method-block-01-static-40-end", "wall_time_ns": 3_000_000_000},
            ]
            (run / "events.ndjson").write_text(
                "".join(json.dumps(row) + "\n" for row in events), encoding="utf-8"
            )
            raw = 'CSI_DATA,1,aa,-40,1,1,0,0,0,0,0,0,0,0,-95,0,6,0,100,0,128,0,2,0,"[1,2]"'
            envelopes = [
                {"ingest_wall_time_ns": 2_000_000_000, "raw": raw},
                {"ingest_wall_time_ns": 4_000_000_000, "raw": raw},
            ]
            with gzip.open(source / "chunk.ndjson.gz", "wt", encoding="utf-8") as stream:
                for envelope in envelopes:
                    stream.write(json.dumps(envelope) + "\n")
            report = MODULE.analyze(run)
        self.assertEqual(report["phases"][0]["csi_frames"], 1)
        self.assertEqual(report["phases"][0]["valid_csi_frames"], 1)
        self.assertEqual(report["phases"][0]["achieved_csi_hz"], 0.5)


if __name__ == "__main__":
    unittest.main()
