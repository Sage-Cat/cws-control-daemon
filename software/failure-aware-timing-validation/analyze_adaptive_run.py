#!/usr/bin/env python3
"""Analyze a finalized adaptive cooperative-sensing validation run."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import fmean
from typing import Any

SOFTWARE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(SOFTWARE_ROOT / "cws-control-daemon"))

from analyze_natural_run import RunningStats, analyze, write_outputs  # noqa: E402
from cws_control.reporting import write_reports  # noqa: E402


def jsonl(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    values = []
    errors = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            values.append(json.loads(line))
        except json.JSONDecodeError as first:
            start = line.find("{")
            try:
                if start < 0:
                    raise first
                values.append(json.loads(line[start:]))
                errors.append({
                    "line": line_number,
                    "recovered": True,
                    "prefix_bytes": start,
                    "prefix": line[: min(start, 200)],
                })
            except json.JSONDecodeError:
                errors.append({
                    "line": line_number, "recovered": False, "error": str(first), "prefix": line[:200]
                })
    return values, errors


def write_clean_events(source: Path, destination: Path) -> dict[str, Any]:
    values, errors = jsonl(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        "".join(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n" for value in values),
        encoding="utf-8",
    )
    return {
        "records": len(values),
        "event_types": dict(sorted(Counter(str(value.get("event_type", "missing")) for value in values).items())),
        "malformed_lines": errors,
    }


def in_window(value: dict[str, Any], field: str, start_ns: int, end_ns: int) -> bool:
    try:
        return start_ns <= int(value[field]) <= end_ns
    except (KeyError, TypeError, ValueError):
        return False


def summarize_prpl(path: Path, start_ns: int, end_ns: int) -> dict[str, Any]:
    values, errors = jsonl(path)
    values = [value for value in values if in_window(value, "sampled_wall_time_ns", start_ns, end_ns)]
    utilization = RunningStats()
    noise = RunningStats()
    associated = RunningStats()
    loads: Counter[str] = Counter()
    quality: Counter[str] = Counter()
    channels: Counter[int] = Counter()
    versions: Counter[str] = Counter()
    valid = 0
    first_ns = last_ns = None
    for value in values:
        wall = int(value.get("sampled_wall_time_ns", 0))
        first_ns = wall if first_ns is None else min(first_ns, wall)
        last_ns = wall if last_ns is None else max(last_ns, wall)
        valid += int(bool(value.get("telemetry_valid")))
        loads[str(value.get("load", "missing"))] += 1
        quality[str(value.get("quality", "missing"))] += 1
        if value.get("agent_version"):
            versions[str(value["agent_version"])] += 1
        for item in value.get("radio_channel_utilization_percent_values", []):
            utilization.add(float(item))
        for item in value.get("radio_noise_dbm_values", []):
            noise.add(float(item))
        for item in value.get("radio_channels", []):
            channels[int(item)] += 1
        if value.get("associated_device_count") is not None:
            associated.add(float(value["associated_device_count"]))
    return {
        "records": len(values),
        "json_errors": errors,
        "valid_records": valid,
        "span_seconds": ((last_ns - first_ns) / 1e9) if first_ns is not None and last_ns is not None else 0,
        "load_counts": dict(sorted(loads.items())),
        "quality_counts": dict(sorted(quality.items())),
        "agent_versions": dict(sorted(versions.items())),
        "channel_counts": {str(key): count for key, count in sorted(channels.items())},
        "channel_utilization_percent": utilization.result(),
        "noise_dbm": noise.result(),
        "associated_devices": associated.result(),
    }


def summarize_context(path: Path, start_ns: int, end_ns: int) -> dict[str, Any]:
    values, errors = jsonl(path)
    values = [value for value in values if in_window(value, "sampled_wall_time_ns", start_ns, end_ns)]
    return {
        "records": len(values),
        "json_errors": errors,
        "load_counts": dict(sorted(Counter(str(value.get("load", "missing")) for value in values).items())),
        "quality_counts": dict(sorted(Counter(str(value.get("quality", "missing")) for value in values).items())),
        "problem_counts": dict(sorted(Counter(
            str(problem) for value in values for problem in value.get("problems", [])
        ).items())),
    }


SIGNAL = re.compile(r"\bsignal:\s*(-?\d+)\s+dBm")
FREQUENCY = re.compile(r"\bfreq:\s*([0-9.]+)")


def summarize_link_sampler(path: Path) -> dict[str, Any]:
    values, errors = jsonl(path)
    signal = RunningStats()
    frequencies: Counter[str] = Counter()
    returncodes: Counter[int] = Counter()
    timed_out = 0
    for value in values:
        output = str(value.get("stdout", ""))
        match = SIGNAL.search(output)
        if match:
            signal.add(float(match.group(1)))
        match = FREQUENCY.search(output)
        if match:
            frequencies[match.group(1)] += 1
        returncodes[int(value.get("returncode", -1))] += 1
        timed_out += int(bool(value.get("timed_out")))
    return {
        "records": len(values),
        "json_errors": errors,
        "signal_dbm": signal.result(),
        "frequency_counts_mhz": dict(sorted(frequencies.items())),
        "returncode_counts": {str(key): count for key, count in sorted(returncodes.items())},
        "timed_out": timed_out,
    }


def decision_stream_diagnostics(
    path: Path, start_ns: int | None = None, end_ns: int | None = None,
) -> dict[str, Any]:
    values, errors = jsonl(path)
    if start_ns is not None and end_ns is not None:
        values = [value for value in values if in_window(value, "decision_wall_time_ns", start_ns, end_ns)]
    modes: Counter[str] = Counter()
    sequences = []
    walls = []
    actuation: Counter[str] = Counter()
    applied_hz: Counter[int] = Counter()
    segments: list[dict[str, Any]] = []
    current_segment: dict[str, Any] | None = None
    for value in values:
        modes[str(value.get("mode", "missing"))] += 1
        sequences.append(int(value.get("decision_sequence", 0)))
        walls.append(int(value.get("decision_wall_time_ns", 0)))
        sequence = int(value.get("decision_sequence", 0))
        wall = int(value.get("decision_wall_time_ns", 0))
        if current_segment is None or sequence <= int(current_segment["last_sequence"]):
            current_segment = {
                "segment": len(segments) + 1,
                "records": 0,
                "first_wall_time_ns": wall,
                "last_wall_time_ns": wall,
                "first_sequence": sequence,
                "last_sequence": sequence,
                "mode_counts": Counter(),
            }
            segments.append(current_segment)
        current_segment["records"] += 1
        current_segment["last_wall_time_ns"] = wall
        current_segment["last_sequence"] = sequence
        current_segment["mode_counts"][str(value.get("mode", "missing"))] += 1
        for action in value.get("actuation", []):
            status = str(action.get("status", "missing"))
            actuation[status] += 1
            if status in {"applied", "success"} and action.get("hz") is not None:
                applied_hz[int(action["hz"])] += 1
    public_segments = []
    overlap_seconds = 0.0
    previous_latest_end: int | None = None
    for segment in segments:
        start = int(segment["first_wall_time_ns"])
        end = int(segment["last_wall_time_ns"])
        overlap = max(0, (previous_latest_end or start) - start) / 1e9
        overlap_seconds += overlap
        previous_latest_end = max(previous_latest_end or end, end)
        public_segments.append({
            **{key: value for key, value in segment.items() if key != "mode_counts"},
            "mode_counts": dict(sorted(segment["mode_counts"].items())),
            "overlap_with_earlier_segments_seconds": overlap,
        })
    return {
        "records": len(values),
        "json_errors": errors,
        "mode_counts": dict(sorted(modes.items())),
        "sequence_regressions": sum(right <= left for left, right in zip(sequences, sequences[1:])),
        "wall_time_regressions": sum(right < left for left, right in zip(walls, walls[1:])),
        "inferred_process_segments": 1 + sum(right <= left for left, right in zip(sequences, sequences[1:])),
        "process_segments": public_segments,
        "overlapping_process_time_seconds": overlap_seconds,
        "actuation_status_counts": dict(sorted(actuation.items())),
        "applied_rate_counts_hz": {str(key): count for key, count in sorted(applied_hz.items())},
    }


def iso_to_ns(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1e9)


def write_decision_window(source: Path, destination: Path, start_ns: int, end_ns: int) -> int:
    values, errors = jsonl(source)
    if errors:
        raise ValueError(f"controller decision JSON errors: {errors[:3]}")
    selected = [
        value for value in values
        if in_window(value, "decision_wall_time_ns", start_ns, end_ns)
    ]
    selected.sort(key=lambda value: (int(value["decision_wall_time_ns"]), int(value["decision_sequence"])))
    destination.write_text(
        "".join(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n" for value in selected),
        encoding="utf-8",
    )
    return len(selected)


def markdown(summary: dict[str, Any]) -> str:
    controller = summary["controller"]
    stream = summary["controller_stream"]
    lines = [
        "# Adaptive cooperative-sensing validation",
        "",
        f"The finalized inputs contain {len(summary['nodes'])} collector streams and "
        f"{controller['decisions']:,} controller "
        f"decisions over {controller['duration_seconds'] / 3600:.2f} hours.",
        "",
        "## Node evidence",
        "",
    ]
    for node, report in summary["nodes"].items():
        lines.append(
            f"- {node}: {report['records']['total_envelopes']:,} envelopes, "
            f"{report['records']['valid_csi']:,} valid CSI, "
            f"{report['records']['malformed_csi']:,} malformed CSI, "
            f"{len(report['identity']['boot_epochs'])} reported boot epochs, "
            f"{report['timing']['timestamp_wraps']} timestamp wraps."
        )
    lines.extend([
        "",
        "## Controller and prplOS evidence",
        "",
        f"- Hold ratio: {controller['hold_ratio'] * 100:.2f}%; configuration changes: "
        f"{controller['configuration_changes']} ({controller['configuration_changes_per_minute']:.3f}/min).",
        f"- Mean predicted sensing airtime: {controller['mean_airtime_percent']:.4f}%; mean QoS penalty: "
        f"{controller['mean_qos_penalty_percent']:.4f}%; accepted budget violations: "
        f"{controller['accepted_budget_violation_decisions']}.",
        f"- Modes: {stream['mode_counts']}; actuator results: {stream['actuation_status_counts']}. "
        "No successful configuration actuation is present." if not stream["applied_rate_counts_hz"] else
        f"- Applied sensing rates: {stream['applied_rate_counts_hz']}.",
        f"- Controller process segments: {stream['inferred_process_segments']}; overlapping process time: "
        f"{stream['overlapping_process_time_seconds']:.3f} s.",
        f"- prplOS telemetry: {summary['prpl']['valid_records']:,}/{summary['prpl']['records']:,} valid records; "
        f"mean channel utilization {summary['prpl']['channel_utilization_percent']['mean']:.3f}%.",
        "",
        "## Claim boundary",
        "",
        "These outputs quantify implementation continuity, timing/failure gates, cooperative admission, "
        "budget accounting, maintenance, actuator accounting, and network-context integration. Unlabelled "
        "natural-operation data does not by itself establish occupancy/activity accuracy or causal QoS gains.",
        "",
    ])
    return "\n".join(lines)


def node_argument(value: str) -> tuple[str, Path]:
    source_id, separator, path = value.partition("=")
    if not separator or not source_id or not path:
        raise argparse.ArgumentTypeError("node must use SOURCE_ID=RUN_DIRECTORY")
    return source_id, Path(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", action="append", type=node_argument, required=True)
    parser.add_argument("--window-manifest", type=Path, required=True)
    parser.add_argument("--controller-decisions", type=Path, required=True)
    parser.add_argument("--network-context", type=Path, required=True)
    parser.add_argument("--radio-context", type=Path, required=True)
    parser.add_argument("--method-block-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-every", type=int, default=100)
    args = parser.parse_args()
    if args.sample_every < 1:
        parser.error("--sample-every must be positive")
    node_runs = dict(args.node)
    if len(node_runs) != len(args.node):
        parser.error("--node source IDs must be unique")
    args.output.mkdir(parents=True, exist_ok=True)

    run_manifest = json.loads(args.window_manifest.read_text(encoding="utf-8"))
    run_start_ns = iso_to_ns(run_manifest["started_at"])
    run_end_ns = iso_to_ns(run_manifest["target_end_at"])

    nodes = {}
    clean_event_paths = []
    event_accounting = {}
    link_context = {}
    for node, run in sorted(node_runs.items()):
        report = analyze(run, sample_every=args.sample_every)
        write_outputs(report, args.output / "nodes" / node)
        nodes[node] = report
        clean = args.output / "normalized-events" / f"{node}.ndjson"
        event_accounting[node] = write_clean_events(run / "events.ndjson", clean)
        clean_event_paths.append(clean)
        link_context[node] = summarize_link_sampler(run / "samplers" / "wifi-link-context.ndjson")

    decision_window = args.output / "controller-decisions-run-window.ndjson"
    window_decisions = write_decision_window(
        args.controller_decisions, decision_window, run_start_ns, run_end_ns
    )
    controller = write_reports(
        decision_window,
        args.output / "controller-summary.json",
        args.output / "controller-report.md",
        clean_event_paths,
    )
    result = {
        "schema_version": 1,
        "inputs": {
            "nodes": {name: str(path) for name, path in sorted(node_runs.items())},
            "window_manifest": str(args.window_manifest),
            "controller_decisions": str(args.controller_decisions),
            "network_context": str(args.network_context),
            "radio_context": str(args.radio_context),
        },
        "nodes": nodes,
        "events": event_accounting,
        "link_context": link_context,
        "controller": controller,
        "controller_stream": decision_stream_diagnostics(
            args.controller_decisions, run_start_ns, run_end_ns
        ),
        "controller_archive_stream": decision_stream_diagnostics(args.controller_decisions),
        "controller_window": {
            "start_wall_time_ns": run_start_ns,
            "end_wall_time_ns": run_end_ns,
            "decisions": window_decisions,
            "interpretation": "Primary run window; controller records outside it are orchestration margins.",
        },
        "network_context": summarize_context(
            args.network_context, run_start_ns, run_end_ns
        ),
        "prpl": summarize_prpl(args.radio_context, run_start_ns, run_end_ns),
        "method_blocks": (
            json.loads(args.method_block_manifest.read_text(encoding="utf-8"))
            if args.method_block_manifest else None
        ),
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (args.output / "report.md").write_text(markdown(result), encoding="utf-8")
    print(json.dumps({
        "nodes": {node: value["records"]["total_envelopes"] for node, value in nodes.items()},
        "controller_decisions": controller["decisions"],
        "prpl_records": result["prpl"]["records"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
