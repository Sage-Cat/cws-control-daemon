#!/usr/bin/env python3
"""Quantify detection, recovery, quorum, and intervention for campaign trials."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - position) + ordered[upper] * (position - lower)


def bootstrap_median_ci(values: list[float], seed: int = 1, samples: int = 2000) -> list[float] | None:
    if not values:
        return None
    generator = random.Random(seed)
    estimates = [
        median(generator.choices(values, k=len(values)))
        for _ in range(samples)
    ]
    return [float(percentile(estimates, 0.025)), float(percentile(estimates, 0.975))]


def metric_summary(values: list[float]) -> dict[str, Any]:
    return {
        "n": len(values),
        "median": median(values) if values else None,
        "p25": percentile(values, 0.25),
        "p75": percentile(values, 0.75),
        "p95": percentile(values, 0.95),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "bootstrap_median_95ci": bootstrap_median_ci(values),
    }


def load_trials(ledger: Path) -> tuple[list[dict[str, Any]], int, int]:
    events = list(records(ledger))
    campaign_start = next(event for event in events if event["event_type"] == "campaign-start")
    completed = next((event for event in reversed(events) if event["event_type"] == "campaign-complete"), None)
    if completed is None:
        raise ValueError("campaign ledger has no campaign-complete event")
    by_trial: dict[str, dict[str, Any]] = {}
    for event in events:
        trial = event.get("trial")
        if not isinstance(trial, dict):
            continue
        trial_id = str(trial["trial_id"])
        result = by_trial.setdefault(trial_id, {"trial": trial})
        result[event["event_type"]] = event
    expected = int(campaign_start["trial_count"])
    if len(by_trial) != expected:
        raise ValueError(f"expected {expected} trials, found {len(by_trial)}")
    return list(by_trial.values()), int(campaign_start["wall_time_ns"]), int(completed["wall_time_ns"])


def first_after(
    decisions: list[dict[str, Any]],
    start_ns: int,
    end_ns: int,
    predicate: Any,
) -> dict[str, Any] | None:
    return next(
        (
            decision
            for decision in decisions
            if start_ns <= int(decision["decision_wall_time_ns"]) <= end_ns and predicate(decision)
        ),
        None,
    )


def analyze(decisions_path: Path, ledger_path: Path) -> dict[str, Any]:
    trials, campaign_start, campaign_end = load_trials(ledger_path)
    decisions = [
        decision
        for decision in records(decisions_path)
        if campaign_start - 10_000_000_000
        <= int(decision["decision_wall_time_ns"])
        <= campaign_end + 10_000_000_000
    ]
    if not decisions:
        raise ValueError("no controller decisions overlap the campaign")
    individual: list[dict[str, Any]] = []
    for item in trials:
        trial = item["trial"]
        injected_event = item.get("trial-injected")
        stable_event = item.get("trial-stable")
        if not injected_event or not stable_event:
            raise ValueError(f"incomplete trial ledger: {trial['trial_id']}")
        source_id = str(trial["source_id"])
        start_ns = int(
            injected_event["command"].get(
                "truth_boundary_wall_time_ns",
                injected_event["command"]["ended_wall_time_ns"],
            )
        )
        stable_ns = int(stable_event["stability"]["decision_wall_time_ns"])
        detection = first_after(
            decisions,
            start_ns,
            stable_ns,
            lambda decision: not decision["evidence"][source_id]["available"],
        )
        detection_ns = int(detection["decision_wall_time_ns"]) if detection else None
        recovery_origin_ns = start_ns
        restore = item.get("trial-restore-command")
        if restore is not None:
            recovery_origin_ns = int(restore["command"]["ended_wall_time_ns"])
        search_start = detection_ns or start_ns
        timing_recovered = first_after(
            decisions,
            search_start,
            stable_ns,
            lambda decision: bool(decision["evidence"][source_id]["timing_valid"]),
        )
        quorum_recovered = first_after(
            decisions,
            search_start,
            stable_ns,
            lambda decision: int(decision["fusion"]["quorum"]) == len(decision["evidence"]),
        )
        intervention = first_after(
            decisions,
            start_ns,
            stable_ns,
            lambda decision: any(
                action.get("source_id") == source_id and action.get("status") == "applied"
                for action in decision.get("actuation", [])
            ),
        )
        invalid_admitted = sum(
            1
            for decision in decisions
            if start_ns <= int(decision["decision_wall_time_ns"]) <= stable_ns
            and decision["evidence"][source_id]["available"]
            and not decision["evidence"][source_id]["timing_valid"]
        )
        individual.append(
            {
                "trial_id": trial["trial_id"],
                "randomized_sequence": trial["randomized_sequence"],
                "source_id": source_id,
                "fault_type": trial["fault_type"],
                "condition": trial["condition"],
                "start_wall_time_ns": start_ns,
                "detected": detection is not None,
                "detection_latency_seconds": (
                    (detection_ns - start_ns) / 1e9 if detection_ns is not None else None
                ),
                "timing_recovery_latency_seconds": (
                    (int(timing_recovered["decision_wall_time_ns"]) - recovery_origin_ns) / 1e9
                    if timing_recovered is not None
                    else None
                ),
                "full_quorum_recovery_latency_seconds": (
                    (int(quorum_recovered["decision_wall_time_ns"]) - recovery_origin_ns) / 1e9
                    if quorum_recovered is not None
                    else None
                ),
                "system_intervention_latency_seconds": (
                    (int(intervention["decision_wall_time_ns"]) - start_ns) / 1e9
                    if intervention is not None
                    else None
                ),
                "invalid_timing_decisions_admitted": invalid_admitted,
                "stable_wall_time_ns": stable_ns,
            }
        )
    grouped: dict[str, dict[str, Any]] = {}
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for trial in individual:
        buckets[(trial["fault_type"], trial["condition"])].append(trial)
    metrics = (
        "detection_latency_seconds",
        "timing_recovery_latency_seconds",
        "full_quorum_recovery_latency_seconds",
        "system_intervention_latency_seconds",
    )
    for (fault_type, condition), rows in sorted(buckets.items()):
        grouped[f"{fault_type}:{condition}"] = {
            "trials": len(rows),
            "detected": sum(row["detected"] for row in rows),
            "detection_rate": sum(row["detected"] for row in rows) / len(rows),
            "invalid_timing_decisions_admitted": sum(
                row["invalid_timing_decisions_admitted"] for row in rows
            ),
            "metrics": {
                metric: metric_summary(
                    [float(row[metric]) for row in rows if row[metric] is not None]
                )
                for metric in metrics
            },
        }
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "analysis_contract": "failure-aware-timing-campaign-v1",
        "sources": {
            "decisions": {"path": str(decisions_path), "sha256": sha256(decisions_path)},
            "campaign_ledger": {"path": str(ledger_path), "sha256": sha256(ledger_path)},
        },
        "campaign": {
            "start_wall_time_ns": campaign_start,
            "end_wall_time_ns": campaign_end,
            "duration_seconds": (campaign_end - campaign_start) / 1e9,
            "decision_count": len(decisions),
            "trial_count": len(individual),
        },
        "groups": grouped,
        "individual_trials": sorted(individual, key=lambda row: row["randomized_sequence"]),
        "interpretation_boundary": (
            "Latencies measure controller evidence validity, quorum, and rate actuation. "
            "They do not measure human-activity classification accuracy."
        ),
    }


def markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Controlled failure/timing campaign",
        "",
        f"Trials: {report['campaign']['trial_count']}; decisions: {report['campaign']['decision_count']}; "
        f"duration: {report['campaign']['duration_seconds'] / 60:.2f} min.",
        "",
        "| Fault/condition | Detected | Detection median (s) | Timing recovery median (s) "
        "| Quorum recovery median (s) | Applied intervention median (s) | Invalid admitted |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, group in report["groups"].items():
        metrics = group["metrics"]
        lines.append(
            f"| {name} | {group['detected']}/{group['trials']} | "
            f"{metrics['detection_latency_seconds']['median']} | "
            f"{metrics['timing_recovery_latency_seconds']['median']} | "
            f"{metrics['full_quorum_recovery_latency_seconds']['median']} | "
            f"{metrics['system_intervention_latency_seconds']['median']} | "
            f"{group['invalid_timing_decisions_admitted']} |"
        )
    lines.extend(["", report["interpretation_boundary"], ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    report = analyze(args.decisions, args.ledger)
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown:
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(markdown(report), encoding="utf-8")
    print(json.dumps(report["campaign"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
