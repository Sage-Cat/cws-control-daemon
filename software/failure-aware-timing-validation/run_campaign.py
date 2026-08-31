#!/usr/bin/env python3
"""Run a seeded, ground-truthed multi-source failure campaign.

The ledger is authoritative for injection commands. Recovery observations are
derived later from raw firmware PROFILE/heartbeat/CSI envelopes, not from this
runner's wall-clock polling. The ledger is always written below a concrete
private experiment run directory; this helper has no repository-local output.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HELPER_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = HELPER_ROOT.parents[1]
RESULTS_DIRECTORY = Path("results") / "failure_aware_timing_validation"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def render(template: list[str], **values: object) -> list[str]:
    return [
        item.format_map({key: str(value) for key, value in values.items()})
        for item in template
    ]


def command(template: list[str], timeout: float = 15.0, **values: object) -> dict[str, Any]:
    argv = render(template, **values)
    started_ns = time.time_ns()
    try:
        completed = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return {
            "argv": argv,
            "started_wall_time_ns": started_ns,
            "ended_wall_time_ns": time.time_ns(),
            "returncode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
            "status": "ok" if completed.returncode == 0 else "failed",
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "argv": argv,
            "started_wall_time_ns": started_ns,
            "ended_wall_time_ns": time.time_ns(),
            "status": "error",
            "error": str(exc),
        }


def last_json_record(path: Path, maximum: int = 262_144) -> dict[str, Any] | None:
    try:
        with path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - maximum))
            lines = stream.read().splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            return json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    return None


class Campaign:
    def __init__(
        self,
        config: dict[str, Any],
        *,
        ledger: Path,
        decisions: Path,
        actuation_inhibit_file: Path,
    ):
        if config.get("schema_version") != 1:
            raise ValueError("campaign config must use schema_version 1")
        self.config = config
        self.nodes: dict[str, dict[str, Any]] = config["nodes"]
        if not self.nodes:
            raise ValueError("campaign config must define at least one source")
        self.ledger = ledger
        self.decisions = decisions
        self.inhibit = actuation_inhibit_file
        self.timeout = float(config.get("command_timeout_seconds", 15.0))
        self.stability_timeout = float(config.get("stability_timeout_seconds", 90.0))
        self.stability_confirmations = int(config.get("stability_confirmations", 3))

    def record(self, event_type: str, **fields: Any) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "event_type": event_type,
            "wall_time": utc_now(),
            "wall_time_ns": time.time_ns(),
            "monotonic_ns": time.monotonic_ns(),
            **fields,
        }
        append_jsonl(self.ledger, value)
        return value

    def mirror_marker(self, phase: str, trial: dict[str, Any]) -> None:
        note_json = json.dumps(
            {
                "trial_id": trial["trial_id"],
                "source_id": trial["source_id"],
                "fault_type": trial["fault_type"],
                "condition": trial["condition"],
                "phase": phase,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        note = "base64url:" + base64.urlsafe_b64encode(note_json.encode()).decode()
        results = {}
        for source_id, definition in sorted(self.nodes.items()):
            results[source_id] = command(
                definition["event_argv"],
                self.timeout,
                source_id=source_id,
                event_type=f"failure-trial-{phase}",
                note=note,
            )
        self.record("marker-mirror", trial_id=trial["trial_id"], phase=phase, results=results)
        failed = [source for source, result in results.items() if result["status"] != "ok"]
        if failed:
            raise RuntimeError(f"failed to mirror {phase} marker to: {failed}")

    def set_rate(self, source_id: str, hz: int) -> dict[str, Any]:
        result = command(
            self.nodes[source_id]["rate_argv"],
            self.timeout,
            source_id=source_id,
            hz=hz,
        )
        if result["status"] != "ok":
            raise RuntimeError(f"rate {hz} command failed for {source_id}: {result}")
        return result

    def reboot(self, source_id: str) -> dict[str, Any]:
        result = command(
            self.nodes[source_id]["reboot_argv"],
            self.timeout,
            source_id=source_id,
        )
        if result["status"] != "ok":
            raise RuntimeError(f"reboot command failed for {source_id}: {result}")
        return result

    def wait_stable(self) -> dict[str, Any]:
        deadline = time.monotonic() + self.stability_timeout
        consecutive = 0
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = last_json_record(self.decisions)
            fresh = bool(
                last
                and time.time_ns() - int(last.get("decision_wall_time_ns", 0)) < 10_000_000_000
            )
            evidence = last.get("evidence", {}) if last else {}
            stable = bool(
                fresh
                and int(last.get("fusion", {}).get("quorum", -1)) == len(self.nodes)
                and all(
                    evidence.get(source_id, {}).get("available")
                    and evidence.get(source_id, {}).get("timing_valid")
                    for source_id in self.nodes
                )
            )
            consecutive = consecutive + 1 if stable else 0
            if consecutive >= self.stability_confirmations:
                return {
                    "status": "stable",
                    "decision_wall_time_ns": last["decision_wall_time_ns"],
                    "decision_sequence": last.get("decision_sequence"),
                    "confirmations": consecutive,
                }
            time.sleep(1.0)
        raise RuntimeError(f"two-node timing-valid quorum did not stabilize; last={last}")

    def wait_for_detection(self, source_id: str, timeout: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = last_json_record(self.decisions)
            item = last.get("evidence", {}).get(source_id, {}) if last else {}
            if last and not item.get("available", True):
                return {
                    "status": "detected",
                    "decision_wall_time_ns": last["decision_wall_time_ns"],
                    "decision_sequence": last.get("decision_sequence"),
                    "reasons": item.get("unavailable_reasons", []),
                }
            time.sleep(0.5)
        return {"status": "not-observed-within-timeout", "last": last}

    def build_trials(self) -> list[dict[str, Any]]:
        trials: list[dict[str, Any]] = []
        index = 0
        for trial_class in self.config["trial_classes"]:
            repetitions = int(trial_class["repetitions_per_node"])
            for source_id in sorted(self.nodes):
                for repetition in range(1, repetitions + 1):
                    index += 1
                    trials.append(
                        {
                            **trial_class,
                            "source_id": source_id,
                            "repetition": repetition,
                            "trial_id": f"auto-{index:03d}",
                        }
                    )
        random.Random(int(self.config["seed"])).shuffle(trials)
        for sequence, trial in enumerate(trials, 1):
            trial["randomized_sequence"] = sequence
        return trials

    def run_trial(self, trial: dict[str, Any]) -> None:
        source_id = trial["source_id"]
        inhibit = trial["condition"] == "observe-inhibited"
        self.record("trial-precondition", trial=trial, stability=self.wait_stable())
        if inhibit:
            self.inhibit.parent.mkdir(parents=True, exist_ok=True)
            self.inhibit.write_text(
                f"{trial['trial_id']} {utc_now()}\n",
                encoding="utf-8",
            )
        self.mirror_marker("start", trial)
        self.record("trial-start", trial=trial, actuation_inhibited=inhibit)
        if trial["fault_type"] == "sensing-silence":
            injected = self.set_rate(source_id, 0)
        elif trial["fault_type"] == "esp-reboot":
            injected = self.reboot(source_id)
        else:
            raise ValueError(f"unsupported fault type: {trial['fault_type']}")
        self.record("trial-injected", trial=trial, command=injected)
        detection = self.wait_for_detection(source_id, float(trial["detection_timeout_seconds"]))
        self.record("trial-detection-observation", trial=trial, observation=detection)
        if trial["fault_type"] == "sensing-silence":
            remaining = max(
                0.0,
                float(trial["fault_seconds"])
                - (time.time_ns() - int(injected["ended_wall_time_ns"])) / 1e9,
            )
            time.sleep(remaining)
            restored = self.set_rate(
                source_id,
                int(trial.get("restore_rate_hz", self.config["safe_rate_hz"])),
            )
            self.record("trial-restore-command", trial=trial, command=restored)
            self.mirror_marker("restore-command-acked", trial)
        if inhibit:
            self.inhibit.unlink(missing_ok=True)
        stability = self.wait_stable()
        self.record("trial-stable", trial=trial, stability=stability)
        self.mirror_marker("end", trial)
        time.sleep(float(trial["stable_seconds"]))

    def restore_safe_state(self) -> dict[str, Any]:
        self.inhibit.unlink(missing_ok=True)
        restored = {}
        safe_rate_hz = int(self.config["safe_rate_hz"])
        for source_id in sorted(self.nodes):
            try:
                restored[source_id] = self.set_rate(source_id, safe_rate_hz)
            except RuntimeError as exc:
                restored[source_id] = {"status": "failed", "error": str(exc)}
        return restored

    def run(self) -> None:
        if self.ledger.exists() and self.ledger.stat().st_size:
            raise RuntimeError(f"refusing to overwrite existing ledger: {self.ledger}")
        trials = self.build_trials()
        self.record(
            "campaign-start",
            seed=int(self.config["seed"]),
            trial_count=len(trials),
            randomized_trials=trials,
        )
        try:
            self.record("campaign-precondition", stability=self.wait_stable())
            for trial in trials:
                self.run_trial(trial)
        except BaseException as exc:
            restored = self.restore_safe_state()
            self.record("campaign-aborted", error=repr(exc), safe_restore=restored)
            raise
        restored = self.restore_safe_state()
        self.record("campaign-complete", safe_restore=restored)


def external_results_directory(run_dir: Path) -> Path:
    """Resolve the helper-owned result folder below an external run directory."""

    absolute = Path(os.path.abspath(run_dir.expanduser()))
    resolved = absolute.resolve()
    if absolute != resolved:
        raise ValueError("--run-dir and its ancestors must not traverse a symlink")
    if not resolved.is_dir():
        raise ValueError("--run-dir must be an existing external run directory")
    repository = REPOSITORY_ROOT.resolve()
    if resolved == repository or repository in resolved.parents:
        raise ValueError("--run-dir must be outside the repository root")
    if not (resolved / "run.toml").is_file():
        raise ValueError("--run-dir must contain run.toml")

    results_root = resolved / "results"
    destination = resolved / RESULTS_DIRECTORY
    if results_root.is_symlink() or destination.is_symlink():
        raise ValueError("external run result destination must not traverse a symlink")
    destination.mkdir(parents=True, exist_ok=True)
    if resolved not in destination.resolve().parents:
        raise ValueError("external run result destination escaped --run-dir")
    return destination


def add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--run-dir",
        type=Path,
        required=True,
        help="Existing external run directory containing run.toml",
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        required=True,
        help="Live controller decision stream used only as an input",
    )
    parser.add_argument(
        "--actuation-inhibit-file",
        type=Path,
        required=True,
        help="Explicit runtime interlock path; no default is assumed",
    )


def campaign_from_arguments(
    args: argparse.Namespace,
    campaign_type: type[Campaign] = Campaign,
    ledger_name: str = "campaign-ledger.ndjson",
) -> Campaign:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    results = external_results_directory(args.run_dir)
    return campaign_type(
        config,
        ledger=results / ledger_name,
        decisions=args.decisions.resolve(),
        actuation_inhibit_file=args.actuation_inhibit_file.resolve(),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a controlled sensing-source failure campaign")
    add_runtime_arguments(parser)
    args = parser.parse_args()
    campaign_from_arguments(args).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
