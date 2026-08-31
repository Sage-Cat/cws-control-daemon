#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import hashlib
import json
import re
from pathlib import Path
from typing import Any


MARKER = re.compile(r"^method-block-(?P<sequence>[0-9]+)-(?P<method>static-40|aqmc-only|aqmc-sacm)-(?P<phase>start|end)$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def parse_heartbeat(raw: str) -> dict[str, str]:
    return dict(token.split("=", 1) for token in raw.split()[1:] if "=" in token)


def analyze(run_dir: Path) -> dict[str, Any]:
    events_path = run_dir / "events.ndjson"
    markers: dict[int, dict[str, Any]] = {}
    with events_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            match = MARKER.match(str(event.get("event_type", "")))
            if not match:
                continue
            sequence = int(match.group("sequence"))
            markers.setdefault(sequence, {"sequence": sequence, "method": match.group("method")})[
                f"{match.group('phase')}_wall_time_ns"
            ] = int(event["wall_time_ns"])
    phases = [markers[key] for key in sorted(markers)]
    if not phases or any("start_wall_time_ns" not in phase or "end_wall_time_ns" not in phase for phase in phases):
        raise ValueError("method block markers are incomplete")
    starts = [phase["start_wall_time_ns"] for phase in phases]
    for phase in phases:
        phase.update(
            {
                "duration_seconds": (phase["end_wall_time_ns"] - phase["start_wall_time_ns"]) / 1e9,
                "records": 0,
                "csi_frames": 0,
                "valid_csi_frames": 0,
                "heartbeat_records": 0,
                "profile_records": 0,
                "boot_epochs": set(),
                "chip_temperatures_c": [],
            }
        )
    source_dirs = [path for path in (run_dir / "sources").iterdir() if path.is_dir()]
    if len(source_dirs) != 1:
        raise ValueError(f"expected one source directory, found {source_dirs}")
    source_id = source_dirs[0].name
    chunks = sorted(source_dirs[0].glob("*.ndjson.gz"))
    for path in chunks:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            for line in stream:
                envelope = json.loads(line)
                wall_ns = int(envelope["ingest_wall_time_ns"])
                index = bisect.bisect_right(starts, wall_ns) - 1
                if index < 0:
                    continue
                phase = phases[index]
                if wall_ns > phase["end_wall_time_ns"]:
                    continue
                phase["records"] += 1
                raw = str(envelope["raw"])
                if raw.startswith("CSI_DATA,"):
                    phase["csi_frames"] += 1
                    try:
                        row = next(csv.reader([raw]))
                        if len(row) == 25 and int(row[21]) == 0 and int(row[23]) == 0 and int(row[22]) > 0:
                            phase["valid_csi_frames"] += 1
                    except (csv.Error, StopIteration, ValueError):
                        pass
                elif raw.startswith("CWSLAB_TIMING_HEARTBEAT "):
                    phase["heartbeat_records"] += 1
                    values = parse_heartbeat(raw)
                    if values.get("boot_epoch", "").isdigit():
                        phase["boot_epochs"].add(int(values["boot_epoch"]))
                    try:
                        phase["chip_temperatures_c"].append(int(values["chip_temp_millicelsius"]) / 1000)
                    except (KeyError, ValueError):
                        pass
                elif raw.startswith("CSI_PROFILE "):
                    phase["profile_records"] += 1
    for phase in phases:
        phase["achieved_csi_hz"] = phase["csi_frames"] / phase["duration_seconds"]
        phase["valid_csi_fraction"] = phase["valid_csi_frames"] / max(1, phase["csi_frames"])
        phase["boot_epochs"] = sorted(phase["boot_epochs"])
        temperatures = phase.pop("chip_temperatures_c")
        phase["temperature_c_mean"] = sum(temperatures) / len(temperatures) if temperatures else None
        phase["temperature_c_max"] = max(temperatures) if temperatures else None
    return {
        "schema_version": 1,
        "run_dir": str(run_dir),
        "events_sha256": sha256(events_path),
        "source_id": source_id,
        "chunk_count": len(chunks),
        "phases": phases,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.run_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"source_id": result["source_id"], "phases": len(result["phases"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
