from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from statistics import fmean
from typing import Any, Iterable

from cws_control.context import atomic_write, utc_now


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


FAILURE_EVENT = re.compile(r"^failure-(?P<source>.+)-(?P<phase>start|reconnected)$")


def load_operator_events(paths: Iterable[Path]) -> tuple[list[dict[str, Any]], dict[str, dict[str, list[int]]]]:
    events: list[dict[str, Any]] = []
    markers: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for path in paths:
        for event in records(path):
            event_type = str(event.get("event_type", ""))
            wall_ns = event.get("wall_time_ns")
            if wall_ns is None:
                continue
            normalized = {
                "event_type": event_type,
                "wall_time_ns": int(wall_ns),
                "source_path": str(path),
            }
            events.append(normalized)
            match = FAILURE_EVENT.match(event_type)
            if match:
                markers[match.group("source")][match.group("phase")].append(int(wall_ns))
    return sorted(events, key=lambda item: item["wall_time_ns"]), markers


def summarize_decisions(path: Path, event_paths: Iterable[Path] = ()) -> dict[str, Any]:
    operator_events, failure_markers = load_operator_events(event_paths)
    action_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    quorum_counts: Counter[int] = Counter()
    load_counts: Counter[str] = Counter()
    timing_event_counts: Counter[str] = Counter()
    actuation_counts: Counter[str] = Counter()
    node_available: Counter[str] = Counter()
    node_total: Counter[str] = Counter()
    node_unavailable_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    confidences: dict[str, list[float]] = defaultdict(list)
    timing_uncertainties: dict[str, list[float]] = defaultdict(list)
    airtimes: list[float] = []
    penalties: list[float] = []
    motion_scores: list[float] = []
    first_ns = last_ns = None
    decisions = budget_violations = accepted_budget_violations = 0
    estimated_update_bytes = 0
    unknown_update_estimates = 0
    positive_hold_regret = 0.0
    longest_deadlock = current_deadlock = 0
    configuration_changes = rate_tier_changes = membership_changes = 0
    previous_tiers: dict[str, str] | None = None
    previous_availability: dict[str, bool] = {}
    open_outages: dict[str, dict[str, Any]] = {}
    availability_outages: dict[str, list[dict[str, Any]]] = defaultdict(list)
    failure_results: dict[str, dict[str, Any]] = {}
    for source_id, phases in failure_markers.items():
        if phases.get("start") and phases.get("reconnected"):
            failure_results[source_id] = {
                "start_wall_time_ns": min(phases["start"]),
                "start_marker_spread_ms": (max(phases["start"]) - min(phases["start"])) / 1e6,
                "reconnected_wall_time_ns": max(phases["reconnected"]),
                "reconnected_marker_spread_ms": (
                    max(phases["reconnected"]) - min(phases["reconnected"])
                ) / 1e6,
                "detection_wall_time_ns": None,
                "timing_recovered_wall_time_ns": None,
                "quorum_recovered_wall_time_ns": None,
                "configuration_readmitted_wall_time_ns": None,
                "reported_boot_epoch_change_wall_time_ns": None,
            }
    for record in records(path):
        decisions += 1
        wall_ns = int(record["decision_wall_time_ns"])
        first_ns = wall_ns if first_ns is None else min(first_ns, wall_ns)
        last_ns = wall_ns if last_ns is None else max(last_ns, wall_ns)
        sacm = record["sacm"]
        action_counts[str(sacm["action"])] += 1
        reason_counts[str(sacm["reason"])] += 1
        quorum_counts[int(record["fusion"]["quorum"])] += 1
        load_counts[str(record["load"])] += 1
        airtimes.append(float(record["aqmc"]["budget"]["airtime_percent"]))
        penalties.append(float(record["aqmc"]["budget"]["qos_penalty_percent"]))
        motion_scores.append(float(record["fusion"]["motion_score"]))
        feasible = bool(record["aqmc"]["budget"]["feasible"])
        if not feasible:
            budget_violations += 1
            if sacm["action"] == "accept":
                accepted_budget_violations += 1
        # Pending reconciliation has no valid incumbent-based byte estimate.
        if sacm["estimated_update_bytes"] is not None:
            estimated_update_bytes += int(sacm["estimated_update_bytes"])
        else:
            unknown_update_estimates += 1
        is_deadlock = sacm["action"] == "hold" and bool(sacm["candidate_feasible"]) and float(sacm["utility_gain"]) > 0
        if is_deadlock:
            current_deadlock += 1
            positive_hold_regret += float(sacm["utility_gain"])
            longest_deadlock = max(longest_deadlock, current_deadlock)
        else:
            current_deadlock = 0
        tiers = dict(sacm["configuration"]["tiers"])
        if previous_tiers is not None and tiers != previous_tiers:
            configuration_changes += 1
            nodes = set(tiers) | set(previous_tiers)
            membership_changes += sum((tiers.get(node, "off") == "off") != (previous_tiers.get(node, "off") == "off") for node in nodes)
            rate_tier_changes += sum(
                tiers.get(node, "off") != previous_tiers.get(node, "off")
                and tiers.get(node, "off") != "off"
                and previous_tiers.get(node, "off") != "off"
                for node in nodes
            )
        previous_tiers = tiers
        for event in record.get("timing_events", []):
            timing_event_counts[str(event["event"])] += 1
            source_id = str(event.get("source_id", ""))
            result = failure_results.get(source_id)
            if (
                result
                and event.get("event") in {"reported-boot-epoch-change", "inferred-clock-reset"}
                and int(event.get("ingest_wall_time_ns", wall_ns)) >= result["reconnected_wall_time_ns"]
                and result["reported_boot_epoch_change_wall_time_ns"] is None
            ):
                result["reported_boot_epoch_change_wall_time_ns"] = int(
                    event.get("ingest_wall_time_ns", wall_ns)
                )
        for action in record.get("actuation", []):
            actuation_counts[str(action.get("status", "unknown"))] += 1
        for source_id, item in record["evidence"].items():
            node_total[source_id] += 1
            if item["available"]:
                node_available[source_id] += 1
            for reason in item["unavailable_reasons"]:
                node_unavailable_reasons[source_id][str(reason)] += 1
            confidences[source_id].append(float(item["confidence"]))
            if item["timing_uncertainty_ms"] is not None:
                timing_uncertainties[source_id].append(float(item["timing_uncertainty_ms"]))
            available = bool(item["available"])
            if source_id in previous_availability and previous_availability[source_id] and not available:
                open_outages[source_id] = {
                    "unavailable_wall_time_ns": wall_ns,
                    "unavailable_reasons": list(item.get("unavailable_reasons", [])),
                    "left_censored": False,
                }
            elif source_id not in previous_availability and not available:
                open_outages[source_id] = {
                    "unavailable_wall_time_ns": wall_ns,
                    "unavailable_reasons": list(item.get("unavailable_reasons", [])),
                    "left_censored": True,
                }
            elif previous_availability.get(source_id) is False and available:
                outage = open_outages.pop(source_id, None)
                if outage is not None:
                    outage["available_wall_time_ns"] = wall_ns
                    outage["duration_seconds"] = (
                        wall_ns - int(outage["unavailable_wall_time_ns"])
                    ) / 1e9
                    outage["right_censored"] = False
                    availability_outages[source_id].append(outage)
            previous_availability[source_id] = available

            result = failure_results.get(source_id)
            if result:
                if (
                    wall_ns >= result["start_wall_time_ns"]
                    and not available
                    and result["detection_wall_time_ns"] is None
                ):
                    result["detection_wall_time_ns"] = wall_ns
                if wall_ns >= result["reconnected_wall_time_ns"] and available:
                    if result["timing_recovered_wall_time_ns"] is None and bool(item.get("timing_valid")):
                        result["timing_recovered_wall_time_ns"] = wall_ns
                    if (
                        result["configuration_readmitted_wall_time_ns"] is None
                        and tiers.get(source_id, "off") != "off"
                        and (
                            record.get("mode") != "apply"
                            or "apply_state" not in record
                            or record["apply_state"]["status"] == "confirmed"
                        )
                    ):
                        result["configuration_readmitted_wall_time_ns"] = wall_ns
        for source_id, result in failure_results.items():
            if (
                wall_ns >= result["reconnected_wall_time_ns"]
                and int(record["fusion"]["quorum"]) == len(record["evidence"])
                and result["quorum_recovered_wall_time_ns"] is None
            ):
                result["quorum_recovered_wall_time_ns"] = wall_ns
    if not decisions:
        raise ValueError("decision log is empty")
    for source_id, outage in open_outages.items():
        outage["available_wall_time_ns"] = None
        outage["duration_seconds"] = ((last_ns or 0) - int(outage["unavailable_wall_time_ns"])) / 1e9
        outage["right_censored"] = True
        availability_outages[source_id].append(outage)
    for result in failure_results.values():
        start_ns = result["start_wall_time_ns"]
        reconnect_ns = result["reconnected_wall_time_ns"]
        result["detection_latency_seconds"] = (
            (result["detection_wall_time_ns"] - start_ns) / 1e9
            if result["detection_wall_time_ns"] is not None
            else None
        )
        for name in (
            "reported_boot_epoch_change",
            "timing_recovered",
            "quorum_recovered",
            "configuration_readmitted",
        ):
            wall_key = f"{name}_wall_time_ns"
            result[f"{name}_latency_seconds"] = (
                (result[wall_key] - reconnect_ns) / 1e9 if result[wall_key] is not None else None
            )
    duration_seconds = max(0.0, ((last_ns or 0) - (first_ns or 0)) / 1e9)
    minutes = max(duration_seconds / 60.0, 1 / 60.0)
    node_summary = {}
    for source_id in sorted(node_total):
        node_summary[source_id] = {
            "available_decisions": node_available[source_id],
            "availability_percent": 100.0 * node_available[source_id] / node_total[source_id],
            "mean_confidence": fmean(confidences[source_id]),
            "mean_timing_uncertainty_ms": fmean(timing_uncertainties[source_id]) if timing_uncertainties[source_id] else None,
            "unavailable_reasons": dict(sorted(node_unavailable_reasons[source_id].items())),
        }
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "source": {"path": str(path), "sha256": sha256(path)},
        "decisions": decisions,
        "duration_seconds": duration_seconds,
        "action_counts": dict(sorted(action_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "quorum_counts": {str(key): value for key, value in sorted(quorum_counts.items())},
        "load_counts": dict(sorted(load_counts.items())),
        "mean_airtime_percent": fmean(airtimes),
        "mean_qos_penalty_percent": fmean(penalties),
        "mean_fused_motion_score": fmean(motion_scores),
        "budget_violation_decisions": budget_violations,
        "accepted_budget_violation_decisions": accepted_budget_violations,
        "configuration_changes": configuration_changes,
        "configuration_changes_per_minute": configuration_changes / minutes,
        "membership_changes": membership_changes,
        "rate_tier_changes": rate_tier_changes,
        "hold_ratio": action_counts["hold"] / decisions,
        "estimated_accepted_update_bytes": estimated_update_bytes,
        "unknown_update_estimate_decisions": unknown_update_estimates,
        "positive_hold_regret": positive_hold_regret,
        "longest_positive_gain_hold_run": longest_deadlock,
        "timing_event_counts": dict(sorted(timing_event_counts.items())),
        "actuation_status_counts": dict(sorted(actuation_counts.items())),
        "nodes": node_summary,
        "availability_outages": dict(sorted(availability_outages.items())),
        "operator_events": operator_events,
        "failure_validation": dict(sorted(failure_results.items())),
        "interpretation_boundary": (
            "Controller admission, timing, reliability, stability, and actuation accounting only; "
            "not causal sensing-accuracy, latency, or throughput improvement evidence."
        ),
    }


def markdown_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Cooperative sensing controller validation",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"The log contains {summary['decisions']} controller decisions over "
        f"{summary['duration_seconds'] / 60.0:.2f} minutes. SACM held the accepted "
        f"configuration for {summary['hold_ratio'] * 100:.2f}% of decisions and made "
        f"{summary['configuration_changes']} configuration changes "
        f"({summary['configuration_changes_per_minute']:.3f}/min).",
        "",
        f"Mean predicted sensing airtime was {summary['mean_airtime_percent']:.4f}% and "
        f"mean load-weighted QoS penalty was {summary['mean_qos_penalty_percent']:.4f}%. "
        f"Accepted over-budget decisions: {summary['accepted_budget_violation_decisions']}.",
        "",
        "## Cooperative and failure evidence",
        "",
    ]
    for quorum, count in summary["quorum_counts"].items():
        lines.append(f"- Quorum {quorum}: {count} decisions")
    for source_id, node in summary["nodes"].items():
        lines.append(
            f"- {source_id}: {node['availability_percent']:.2f}% available, "
            f"mean confidence {node['mean_confidence']:.3f}, "
            f"mean timing uncertainty {node['mean_timing_uncertainty_ms']} ms"
        )
    for source_id, result in summary["failure_validation"].items():
        lines.append(
            f"- {source_id} marked failure: detection {result['detection_latency_seconds']} s; "
            f"timing recovery {result['timing_recovered_latency_seconds']} s; "
            f"full-quorum recovery {result['quorum_recovered_latency_seconds']} s; "
            f"configuration re-admission {result['configuration_readmitted_latency_seconds']} s"
        )
    lines.extend(
        [
            "",
            "## Method boundary",
            "",
            summary["interpretation_boundary"],
            "",
            f"Decision log SHA-256: `{summary['source']['sha256']}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_reports(
    input_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    event_paths: Iterable[Path] = (),
) -> dict[str, Any]:
    summary = summarize_decisions(input_path, event_paths)
    atomic_write(json_path, summary)
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_report(summary), encoding="utf-8")
    return summary
