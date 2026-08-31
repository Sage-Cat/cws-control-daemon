from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from cws_control.context import atomic_write, utc_now
from cws_control.engine import CommandActuator


def render_argv(template: list[str], source_id: str, value: str) -> list[str]:
    return [item.replace("{source_id}", source_id).replace("{value}", value) for item in template]


def add_event(definition: Mapping[str, Any], source_id: str, event: str, timeout: float = 10.0) -> dict[str, Any]:
    template = definition.get("event_argv")
    if not isinstance(template, list) or not template:
        return {"source_id": source_id, "event": event, "status": "no-event-actuator"}
    argv = render_argv(template, source_id, event)
    try:
        completed = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
        return {
            "source_id": source_id,
            "event": event,
            "status": "recorded" if completed.returncode == 0 else "failed",
            "returncode": completed.returncode,
            "stdout": completed.stdout[-2000:],
            "stderr": completed.stderr[-2000:],
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"source_id": source_id, "event": event, "status": "error", "error": str(exc)}


def run_rate_sweep(
    config: Mapping[str, Any],
    rates: list[int],
    phase_seconds: float,
    output: Path,
) -> dict[str, Any]:
    if not rates or any(rate < 1 or rate > 50 for rate in rates):
        raise ValueError("sweep rates must be from 1 to 50 Hz")
    if phase_seconds <= 0:
        raise ValueError("phase duration must be positive")
    definitions = config.get("actuators", {})
    if not definitions:
        raise ValueError("rate sweep requires actuator definitions")
    actuator = CommandActuator(definitions, timeout_seconds=10.0)
    report: dict[str, Any] = {
        "schema_version": 1,
        "experiment": "esp32-runtime-rate-validation",
        "started_at": utc_now(),
        "rates_hz": rates,
        "phase_seconds": phase_seconds,
        "nodes": sorted(definitions),
        "phases": [],
        "status": "running",
    }
    atomic_write(output, report)
    for rate in rates:
        label = f"rate-phase-{rate}hz"
        phase: dict[str, Any] = {
            "rate_hz": rate,
            "started_at": utc_now(),
        }
        phase["commands"] = [actuator.apply(source_id, rate) for source_id in sorted(definitions)]
        if not all(command.get("status") == "applied" for command in phase["commands"]):
            phase["status"] = "command-failed"
            report["phases"].append(phase)
            report["status"] = "failed"
            report["ended_at"] = utc_now()
            atomic_write(output, report)
            return report
        phase["start_events"] = [
            add_event(definition, source_id, f"{label}-start")
            for source_id, definition in sorted(definitions.items())
        ]
        if not all(event.get("status") == "recorded" for event in phase["start_events"]):
            phase["status"] = "start-event-failed"
            report["phases"].append(phase)
            report["status"] = "failed"
            report["ended_at"] = utc_now()
            atomic_write(output, report)
            return report
        phase["status"] = "collecting"
        report["phases"].append(phase)
        atomic_write(output, report)
        time.sleep(phase_seconds)
        phase["end_events"] = [
            add_event(definition, source_id, f"{label}-end")
            for source_id, definition in sorted(definitions.items())
        ]
        phase["ended_at"] = utc_now()
        phase["status"] = (
            "complete"
            if all(event.get("status") == "recorded" for event in phase["end_events"])
            else "end-event-failed"
        )
        atomic_write(output, report)
        if phase["status"] != "complete":
            report["status"] = "failed"
            report["ended_at"] = utc_now()
            atomic_write(output, report)
            return report
    report["status"] = "complete"
    report["ended_at"] = utc_now()
    atomic_write(output, report)
    return report
