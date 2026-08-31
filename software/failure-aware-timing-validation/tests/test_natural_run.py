from __future__ import annotations

import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from analyze_natural_run import WRAP_US, analyze, read_ndjson_lenient


class NaturalRunTests(unittest.TestCase):
    def test_recovers_nul_prefixed_json_line(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "health.ndjson"
            path.write_text('\x00\x00{"wall_time_ns":1}\n', encoding="utf-8")
            values, errors = read_ndjson_lenient(path)

        self.assertEqual(values, [{"wall_time_ns": 1}])
        self.assertEqual(len(errors), 1)
        self.assertTrue(errors[0]["recovered"])

    def test_quantifies_wrap_gap_and_malformed_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary) / "run"
            source = run / "sources" / "node"
            source.mkdir(parents=True)
            rows = [
                {"source_id":"node","session_id":"s","connection_epoch":1,"source_sequence":1,
                 "ingest_wall_time_ns":1_000_000_000,"ingest_monotonic_ns":1,"raw":
                 f'CSI_DATA,10,aa,-40,1,0,0,0,0,0,0,0,0,0,-95,0,2,0,{WRAP_US-10},0,2,0,2,0,"[3,4]"'},
                {"source_id":"node","session_id":"s","connection_epoch":1,"source_sequence":2,
                 "ingest_wall_time_ns":1_100_000_000,"ingest_monotonic_ns":2,"raw":
                 'CSI_DATA,11,aa,-41,1,0,0,0,0,0,0,0,0,0,-95,0,2,0,5,0,2,0,2,0,"[6,8]"'},
                {"source_id":"node","session_id":"s","connection_epoch":1,"source_sequence":3,
                 "ingest_wall_time_ns":2_100_000_000,"ingest_monotonic_ns":3,"raw":"CSI_DATA,bad"},
            ]
            chunk = source / "chunk-00000.ndjson.gz"
            with gzip.open(chunk, "wt", encoding="utf-8") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
            (run / "manifest.json").write_text(json.dumps({
                "run_id":"r","started_at":"x","target_end_at":"y","duration_seconds":2
            }), encoding="utf-8")
            (run / "final.json").write_text(json.dumps({
                "ended_at":"z","reason":"duration-complete","sources":{"node":{}}
            }), encoding="utf-8")
            (run / "events.ndjson").write_text("", encoding="utf-8")
            checksum_paths = [chunk, run / "manifest.json", run / "final.json", run / "events.ndjson"]
            (run / "SHA256SUMS").write_text("".join(
                f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(run)}\n"
                for path in checksum_paths
            ), encoding="utf-8")
            report = analyze(run, sample_every=1)
        self.assertEqual(report["records"]["valid_csi"], 2)
        self.assertEqual(report["records"]["malformed_csi"], 1)
        self.assertEqual(report["timing"]["timestamp_wraps"], 1)
        self.assertEqual(report["radio"]["sampled_csi_mean_amplitude"]["mean"], 7.5)
        self.assertEqual(report["continuity"]["valid_csi_gaps"]["maximum_seconds"], 0.1)


if __name__ == "__main__":
    unittest.main()
