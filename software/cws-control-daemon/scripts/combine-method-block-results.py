#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable

from cws_control.reporting import summarize_decisions


def records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def distribution(values: list[float]) -> dict[str, Any]:
    return {
        "n": len(values),
        "mean": fmean(values) if values else None,
        "median": median(values) if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def combine(method_dir: Path, node_paths: list[Path], prpl_history: Path | None) -> dict[str, Any]:
    manifest = json.loads((method_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise ValueError("method block manifest is not complete")
    nodes = [json.loads(path.read_text(encoding="utf-8")) for path in node_paths]
    phase_maps = [{int(phase["sequence"]): phase for phase in node["phases"]} for node in nodes]
    prpl_samples = list(records(prpl_history)) if prpl_history and prpl_history.exists() else []
    phases: list[dict[str, Any]] = []
    for phase_manifest in manifest["phases"]:
        sequence = int(phase_manifest["sequence"])
        method = str(phase_manifest["method"])
        node_phases = [mapping[sequence] for mapping in phase_maps]
        if any(phase["method"] != method for phase in node_phases):
            raise ValueError(f"node marker mismatch in phase {sequence}")
        start_ns = max(int(phase["start_wall_time_ns"]) for phase in node_phases)
        end_ns = min(int(phase["end_wall_time_ns"]) for phase in node_phases)
        prpl_in_window = [
            sample for sample in prpl_samples
            if start_ns <= int(sample.get("sampled_wall_time_ns", 0)) <= end_ns
        ]
        total_csi = sum(int(phase["csi_frames"]) for phase in node_phases)
        total_valid = sum(int(phase["valid_csi_frames"]) for phase in node_phases)
        temperatures = [
            float(phase["temperature_c_max"])
            for phase in node_phases if phase["temperature_c_max"] is not None
        ]
        row: dict[str, Any] = {
            "sequence": sequence,
            "method": method,
            "duration_seconds": (end_ns - start_ns) / 1e9,
            "nodes": node_phases,
            "combined_csi_frames": total_csi,
            "combined_achieved_csi_hz": sum(float(phase["achieved_csi_hz"]) for phase in node_phases),
            "combined_valid_csi_fraction": total_valid / max(1, total_csi),
            "temperature_c_max": max(temperatures) if temperatures else None,
            "prpl_samples": len(prpl_in_window),
            "prpl_load_counts": {
                load: sum(sample.get("load") == load for sample in prpl_in_window)
                for load in ("light", "moderate", "elevated", "high")
            },
        }
        if method == "static-40":
            row.update(
                {
                    "mean_modeled_airtime_percent": 100.0 * 512 * 8 * 80 / (54.0 * 1e6),
                    "configuration_changes": 0,
                    "configuration_changes_per_minute": 0.0,
                    "accepted_budget_violations": 0,
                    "hold_ratio": None,
                    "controller_decisions": 0,
                }
            )
        else:
            decision_path = method_dir / f"method-block-{sequence:02d}-{method}-decisions.ndjson"
            summary = summarize_decisions(decision_path)
            row.update(
                {
                    "decision_log": str(decision_path),
                    "controller_decisions": summary["decisions"],
                    "mean_modeled_airtime_percent": summary["mean_airtime_percent"],
                    "configuration_changes": summary["configuration_changes"],
                    "configuration_changes_per_minute": summary["configuration_changes_per_minute"],
                    "accepted_budget_violations": summary["accepted_budget_violation_decisions"],
                    "hold_ratio": summary["hold_ratio"],
                    "quorum_counts": summary["quorum_counts"],
                }
            )
        phases.append(row)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for phase in phases:
        grouped[phase["method"]].append(phase)
    methods = {}
    metrics = (
        "combined_achieved_csi_hz",
        "combined_valid_csi_fraction",
        "mean_modeled_airtime_percent",
        "configuration_changes_per_minute",
        "temperature_c_max",
    )
    for method, rows in sorted(grouped.items()):
        methods[method] = {
            "blocks": len(rows),
            "accepted_budget_violations": sum(row["accepted_budget_violations"] for row in rows),
            "prpl_samples": sum(row["prpl_samples"] for row in rows),
            "metrics": {
                metric: distribution([float(row[metric]) for row in rows if row[metric] is not None])
                for metric in metrics
            },
        }
    return {
        "schema_version": 1,
        "experiment": "randomized-prpl-context-method-ablation",
        "seed": manifest["seed"],
        "methods": methods,
        "phases": phases,
        "interpretation_boundary": (
            "Measured CSI yield, controller stability, budget accounting, and prplOS-context coverage. "
            "No claim of activity-classification accuracy is made without labeled classes."
        ),
    }


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# prpl-context cooperative sensing method comparison",
        "",
        "| Method | Blocks | CSI Hz mean | Valid CSI fraction mean | Modeled airtime mean (%) | Changes/min mean | Budget violations | prpl samples |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for method, value in report["methods"].items():
        metrics = value["metrics"]
        lines.append(
            f"| {method} | {value['blocks']} | "
            f"{metrics['combined_achieved_csi_hz']['mean']} | "
            f"{metrics['combined_valid_csi_fraction']['mean']} | "
            f"{metrics['mean_modeled_airtime_percent']['mean']} | "
            f"{metrics['configuration_changes_per_minute']['mean']} | "
            f"{value['accepted_budget_violations']} | {value['prpl_samples']} |"
        )
    lines.extend(["", report["interpretation_boundary"], ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--method-dir", type=Path, required=True)
    parser.add_argument("--node", type=Path, action="append", required=True)
    parser.add_argument("--prpl-history", type=Path)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    report = combine(args.method_dir, args.node, args.prpl_history)
    args.json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        args.markdown.write_text(markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
