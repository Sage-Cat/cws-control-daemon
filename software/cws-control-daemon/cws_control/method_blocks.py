from __future__ import annotations

import json
import random
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from cws_control.context import atomic_write, utc_now
from cws_control.engine import CommandActuator, load_controller_config
from cws_control.sweep import add_event


def wait_for_prpl_context(path: Path, maximum_age_seconds: float, timeout_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            last = json.loads(path.read_text(encoding="utf-8"))
            age = max(0.0, (time.time_ns() - int(last["sampled_wall_time_ns"])) / 1e9)
            if (
                age <= maximum_age_seconds
                and last.get("source") == "prplos-tr181-agent"
                and last.get("telemetry_valid") is True
            ):
                return {**last, "age_seconds": age}
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
        time.sleep(1.0)
    raise RuntimeError(f"fresh prplOS context not available; last={last}")


def run_method_blocks(config: Mapping[str, Any], output_dir: Path) -> dict[str, Any]:
    base_path = Path(str(config["controller_config"]))
    base = load_controller_config(base_path)
    definitions = base.get("actuators", {})
    if not definitions:
        raise ValueError("method blocks require controller actuator definitions")
    duration = float(config.get("phase_seconds", 600.0))
    cooldown = float(config.get("cooldown_seconds", 20.0))
    repetitions = int(config.get("repetitions", 3))
    if duration <= 0 or cooldown < 0 or repetitions <= 0:
        raise ValueError("invalid method-block timing")
    if config.get("require_prpl_context", True):
        prpl_precondition = wait_for_prpl_context(
            Path(base["prpl_context_file"]),
            float(base.get("prpl_context_max_age_seconds", 15.0)),
            float(config.get("prpl_wait_seconds", 180.0)),
        )
    else:
        prpl_precondition = {"status": "not-required"}
    methods = ["static-40", "aqmc-only", "aqmc-sacm"] * repetitions
    random.Random(int(config["seed"])).shuffle(methods)
    output_dir.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {
        "schema_version": 1,
        "experiment": "prpl-context-cooperative-sensing-method-blocks",
        "started_at": utc_now(),
        "seed": int(config["seed"]),
        "phase_seconds": duration,
        "cooldown_seconds": cooldown,
        "randomized_methods": methods,
        "prpl_precondition": prpl_precondition,
        "phases": [],
        "status": "running",
    }
    atomic_write(output_dir / "manifest.json", report)
    actuator = CommandActuator(definitions, timeout_seconds=15.0)

    def set_all(hz: int) -> list[dict[str, Any]]:
        results = [actuator.apply(source_id, hz) for source_id in sorted(definitions)]
        if not all(result.get("status") == "applied" for result in results):
            raise RuntimeError(f"failed to set all nodes to {hz} Hz: {results}")
        return results

    def mark(value: str) -> list[dict[str, Any]]:
        results = [
            add_event(definition, source_id, value)
            for source_id, definition in sorted(definitions.items())
        ]
        if not all(result.get("status") == "recorded" for result in results):
            raise RuntimeError(f"failed to record {value}: {results}")
        return results

    try:
        for sequence, method in enumerate(methods, 1):
            phase: dict[str, Any] = {
                "sequence": sequence,
                "method": method,
                "started_at": utc_now(),
                "status": "starting",
            }
            report["phases"].append(phase)
            phase["initial_rate_commands"] = set_all(40)
            marker = f"method-block-{sequence:02d}-{method}"
            phase["start_events"] = mark(f"{marker}-start")
            decision_path = output_dir / f"{marker}-decisions.ndjson"
            if method == "static-40":
                time.sleep(duration)
            else:
                phase_config = deepcopy(base)
                phase_config["mode"] = "apply"
                phase_config["policy_variant"] = method
                config_path = output_dir / f"{marker}-config.json"
                atomic_write(config_path, phase_config)
                completed = subprocess.run(
                    [
                        sys.executable, "-m", "cws_control.cli", "serve",
                        "--config", str(config_path),
                        "--output", str(decision_path),
                        "--duration", str(duration),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=duration + 30.0,
                    check=False,
                )
                phase["controller"] = {
                    "returncode": completed.returncode,
                    "stdout": completed.stdout[-4000:],
                    "stderr": completed.stderr[-4000:],
                    "decisions": str(decision_path),
                }
                if completed.returncode != 0:
                    raise RuntimeError(f"{method} controller failed: {phase['controller']}")
            phase["end_events"] = mark(f"{marker}-end")
            phase["ended_at"] = utc_now()
            phase["status"] = "complete"
            atomic_write(output_dir / "manifest.json", report)
            set_all(40)
            time.sleep(cooldown)
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = repr(exc)
        report["ended_at"] = utc_now()
        atomic_write(output_dir / "manifest.json", report)
        try:
            set_all(40)
        except Exception as restore_exc:
            report["restore_error"] = repr(restore_exc)
            atomic_write(output_dir / "manifest.json", report)
        raise
    report["status"] = "complete"
    report["ended_at"] = utc_now()
    report["final_rate_commands"] = set_all(40)
    atomic_write(output_dir / "manifest.json", report)
    return report
