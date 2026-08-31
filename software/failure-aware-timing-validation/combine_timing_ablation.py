#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def combine(paths: list[Path]) -> dict[str, Any]:
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    modes = reports[0]["methods"].keys()
    methods = {}
    for mode in modes:
        tp = sum(report["methods"][mode]["true_positive"] for report in reports)
        fp = sum(report["methods"][mode]["false_positive"] for report in reports)
        fn = sum(report["methods"][mode]["false_negative"] for report in reports)
        latency_n = sum(report["methods"][mode]["reset_detection_latency"]["n"] for report in reports)
        methods[mode] = {
            "true_positive": tp,
            "false_positive": fp,
            "false_negative": fn,
            "precision": tp / max(1, tp + fp),
            "recall": tp / max(1, tp + fn),
            "false_reset_rate_per_node_hour": fp / max(1e-9, sum(report["duration_hours"] for report in reports)),
            "latency_observations": latency_n,
        }
    return {
        "schema_version": 1,
        "nodes": [report["source_id"] for report in reports],
        "node_hours": sum(report["duration_hours"] for report in reports),
        "planned_reboot_trials": sum(report["planned_reboot_trials"] for report in reports),
        "ground_truth_boot_epoch_transitions": sum(
            report.get("ground_truth_boot_epoch_transitions", report["ground_truth_reset_profiles"])
            for report in reports
        ),
        "ground_truth_reset_profiles": sum(report["ground_truth_reset_profiles"] for report in reports),
        "ground_truth_timestamp_wraps": sum(report["ground_truth_timestamp_wraps"] for report in reports),
        "methods": methods,
    }


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Timing-method ablation",
        "",
        f"Node-hours: {report['node_hours']:.3f}; boot-epoch transitions: "
        f"{report['ground_truth_boot_epoch_transitions']}; "
        f"timestamp wraps: {report['ground_truth_timestamp_wraps']}.",
        "",
        "| Method | TP | FP | FN | Precision | Recall | False resets/node-hour |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for mode, value in report["methods"].items():
        lines.append(
            f"| {mode} | {value['true_positive']} | {value['false_positive']} | "
            f"{value['false_negative']} | {value['precision']} | {value['recall']} | "
            f"{value['false_reset_rate_per_node_hour']} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    report = combine(args.input)
    args.json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        args.markdown.write_text(markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
