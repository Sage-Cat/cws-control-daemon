#!/usr/bin/env python3
"""Run unattended, ground-truthed USB power-loss trials on sensing nodes."""

from __future__ import annotations

import argparse
import random
import time
from typing import Any

from run_campaign import Campaign, add_runtime_arguments, campaign_from_arguments, command


class UsbPowerCampaign(Campaign):
    def power(self, source_id: str, action: str) -> dict[str, Any]:
        result = command(
            self.nodes[source_id][f"power_{action}_argv"],
            self.timeout,
            source_id=source_id,
        )
        if result["status"] != "ok":
            raise RuntimeError(f"USB power {action} failed for {source_id}: {result}")
        return result

    def restore_safe_state(self) -> dict[str, Any]:
        restored = {}
        for source_id in sorted(self.nodes):
            try:
                restored[source_id] = self.power(source_id, "on")
            except RuntimeError as exc:
                restored[source_id] = {"status": "failed", "error": str(exc)}
        return restored

    def build_trials(self) -> list[dict[str, Any]]:
        trials = [
            {
                "trial_id": f"usb-{source_id}-{repetition:02d}",
                "source_id": source_id,
                "fault_type": "usb-power-cycle",
                "condition": "recommend",
                "repetition": repetition,
                "fault_seconds": float(self.config.get("fault_seconds", 15.0)),
            }
            for source_id in sorted(self.nodes)
            for repetition in range(1, int(self.config.get("repetitions_per_node", 5)) + 1)
        ]
        random.Random(int(self.config["seed"])).shuffle(trials)
        for sequence, trial in enumerate(trials, 1):
            trial["randomized_sequence"] = sequence
        return trials

    def run_trial(self, trial: dict[str, Any]) -> None:
        source_id = str(trial["source_id"])
        self.record("trial-precondition", trial=trial, stability=self.wait_stable())
        self.mirror_marker("start", trial)
        self.record("trial-start", trial=trial, actuation_inhibited=False)
        injected = self.power(source_id, "off")
        self.record("trial-injected", trial=trial, command=injected)
        detection = self.wait_for_detection(
            source_id, float(self.config.get("detection_timeout_seconds", 15.0))
        )
        self.record("trial-detection-observation", trial=trial, observation=detection)
        remaining = max(
            0.0,
            float(trial["fault_seconds"])
            - (time.time_ns() - int(injected["ended_wall_time_ns"])) / 1e9,
        )
        time.sleep(remaining)
        restored = self.power(source_id, "on")
        self.record("trial-restore-command", trial=trial, command=restored)
        self.mirror_marker("restore-command-acked", trial)
        stability = self.wait_stable()
        self.record("trial-stable", trial=trial, stability=stability)
        self.mirror_marker("end", trial)
        time.sleep(float(self.config.get("stable_seconds", 20.0)))

    def run(self) -> None:
        if self.ledger.exists() and self.ledger.stat().st_size:
            raise RuntimeError(f"refusing to overwrite existing ledger: {self.ledger}")
        trials = self.build_trials()
        self.record(
            "campaign-start",
            seed=int(self.config["seed"]),
            trial_count=len(trials),
            randomized_trials=trials,
            injection="reviewed per-source power adapter command",
        )
        try:
            self.record("campaign-precondition", stability=self.wait_stable())
            for trial in trials:
                self.run_trial(trial)
        except BaseException as exc:
            restored = self.restore_safe_state()
            self.record("campaign-aborted", error=repr(exc), safe_restore=restored)
            raise
        self.record("campaign-complete", safe_restore=self.restore_safe_state())


def main() -> int:
    parser = argparse.ArgumentParser(description="Run unattended USB power-loss trials")
    add_runtime_arguments(parser)
    args = parser.parse_args()
    campaign_from_arguments(
        args,
        campaign_type=UsbPowerCampaign,
        ledger_name="power-loss-campaign-ledger.ndjson",
    ).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
