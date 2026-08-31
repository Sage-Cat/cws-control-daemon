#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import json
from pathlib import Path
from statistics import median
from typing import Any, Iterable


WRAP_US = 1 << 32
MODES = ("naive-reset-on-regression", "rollover-aware", "failure-aware")


def records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def key_values(raw: str) -> dict[str, str]:
    return dict(token.split("=", 1) for token in raw.split()[1:] if "=" in token)


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def summarize_latencies(values: list[float]) -> dict[str, Any]:
    return {
        "n": len(values),
        "median_ms": median(values) if values else None,
        "p95_ms": percentile(values, 0.95),
        "max_ms": max(values) if values else None,
    }


def match_events(events: list[int], truths: list[int], tolerance_ns: int) -> tuple[int, int, int, list[float]]:
    unmatched = set(range(len(events)))
    matched_truth = 0
    latencies: list[float] = []
    for truth in truths:
        candidates = [index for index in unmatched if truth <= events[index] <= truth + tolerance_ns]
        if not candidates:
            continue
        selected = min(candidates, key=lambda index: events[index])
        unmatched.remove(selected)
        matched_truth += 1
        latencies.append((events[selected] - truth) / 1e6)
    return matched_truth, len(unmatched), len(truths) - matched_truth, latencies


def analyze(run_dir: Path, ledger: Path) -> dict[str, Any]:
    ledger_rows = list(records(ledger))
    campaign_start = next(row for row in ledger_rows if row["event_type"] == "campaign-start")
    campaign_end = next(row for row in reversed(ledger_rows) if row["event_type"] == "campaign-complete")
    planned = [
        row for row in ledger_rows
        if row.get("event_type") == "trial-injected"
        and row.get("trial", {}).get("fault_type") in {"esp-reboot", "esp-hardware-reset"}
    ]
    source_dirs = [path for path in (run_dir / "sources").iterdir() if path.is_dir()]
    if len(source_dirs) != 1:
        raise ValueError("run must contain one source")
    source_id = source_dirs[0].name
    planned = [row for row in planned if row["trial"]["source_id"] == source_id]
    reset_truths: list[int] = []
    wrap_truths: list[int] = []
    events: dict[str, list[int]] = {mode: [] for mode in MODES}
    last_raw: dict[str, int | None] = {mode: None for mode in MODES}
    current_reported_epoch: int | None = None
    last_csi_epoch: int | None = None
    first_wall = last_wall = None
    chunk_paths = sorted(
        path for path in source_dirs[0].iterdir()
        if path.name.endswith((".ndjson.gz", ".ndjson.gz.partial", ".ndjson.gz.incomplete"))
    )
    for path in chunk_paths:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            while True:
                try:
                    line = stream.readline()
                except (EOFError, OSError):
                    break
                if not line:
                    break
                envelope = json.loads(line)
                wall_ns = int(envelope["ingest_wall_time_ns"])
                first_wall = wall_ns if first_wall is None else min(first_wall, wall_ns)
                last_wall = wall_ns if last_wall is None else max(last_wall, wall_ns)
                raw = str(envelope["raw"])
                if raw.startswith("CSI_PROFILE "):
                    values = key_values(raw)
                    try:
                        observed_epoch = int(values["boot_epoch"])
                    except (KeyError, ValueError):
                        observed_epoch = current_reported_epoch
                    if (
                        current_reported_epoch is not None
                        and observed_epoch is not None
                        and observed_epoch != current_reported_epoch
                    ):
                        reset_truths.append(wall_ns)
                    current_reported_epoch = observed_epoch
                    continue
                if raw.startswith("CWSLAB_TIMING_HEARTBEAT "):
                    try:
                        observed_epoch = int(key_values(raw)["boot_epoch"])
                    except (KeyError, ValueError):
                        observed_epoch = current_reported_epoch
                    if (
                        current_reported_epoch is not None
                        and observed_epoch is not None
                        and observed_epoch != current_reported_epoch
                    ):
                        reset_truths.append(wall_ns)
                    current_reported_epoch = observed_epoch
                    continue
                if not raw.startswith("CSI_DATA,"):
                    continue
                try:
                    row = next(csv.reader([raw]))
                    raw_us = int(row[18]) & 0xFFFFFFFF
                except (csv.Error, StopIteration, ValueError, IndexError):
                    continue
                previous_reference = last_raw["rollover-aware"]
                is_wrap = bool(
                    previous_reference is not None
                    and raw_us < previous_reference
                    and previous_reference > int(WRAP_US * 0.75)
                    and raw_us < int(WRAP_US * 0.25)
                )
                if is_wrap:
                    wrap_truths.append(wall_ns)
                previous = last_raw["naive-reset-on-regression"]
                if previous is not None and raw_us < previous:
                    events["naive-reset-on-regression"].append(wall_ns)
                last_raw["naive-reset-on-regression"] = raw_us
                previous = last_raw["rollover-aware"]
                if previous is not None and raw_us < previous and not is_wrap:
                    events["rollover-aware"].append(wall_ns)
                last_raw["rollover-aware"] = raw_us
                epoch_changed = (
                    current_reported_epoch is not None
                    and last_csi_epoch is not None
                    and current_reported_epoch != last_csi_epoch
                )
                previous = last_raw["failure-aware"]
                if epoch_changed:
                    events["failure-aware"].append(wall_ns)
                    last_raw["failure-aware"] = raw_us
                elif previous is not None and raw_us < previous and not is_wrap:
                    events["failure-aware"].append(wall_ns)
                    last_raw["failure-aware"] = raw_us
                else:
                    last_raw["failure-aware"] = raw_us
                if current_reported_epoch is not None:
                    last_csi_epoch = current_reported_epoch
    duration_hours = ((last_wall or 0) - (first_wall or 0)) / 3.6e12
    tolerance_ns = 5_000_000_000
    methods = {}
    for mode in MODES:
        true_positive, false_positive, false_negative, latencies = match_events(
            events[mode], reset_truths, tolerance_ns
        )
        methods[mode] = {
            "reset_events": len(events[mode]),
            "true_positive": true_positive,
            "false_positive": false_positive,
            "false_negative": false_negative,
            "precision": true_positive / max(1, true_positive + false_positive),
            "recall": true_positive / max(1, true_positive + false_negative),
            "false_reset_rate_per_node_hour": false_positive / max(duration_hours, 1e-9),
            "reset_detection_latency": summarize_latencies(latencies),
        }
    return {
        "schema_version": 1,
        "source_id": source_id,
        "run_dir": str(run_dir),
        "campaign_start_wall_time_ns": int(campaign_start["wall_time_ns"]),
        "campaign_end_wall_time_ns": int(campaign_end["wall_time_ns"]),
        "duration_hours": duration_hours,
        "planned_reboot_trials": len(planned),
        "ground_truth_boot_epoch_transitions": len(reset_truths),
        "ground_truth_reset_profiles": len(reset_truths),
        "ground_truth_timestamp_wraps": len(wrap_truths),
        "methods": methods,
        "interpretation": (
            "The first reported boot-epoch transition in either CSI_PROFILE or timing heartbeat is reset ground "
            "truth; a 32-bit high-to-low boundary is wrap ground truth. "
            "The five-second matching window is fixed before analysis."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = analyze(args.run_dir, args.ledger)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"source_id": report["source_id"], "wraps": report["ground_truth_timestamp_wraps"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
