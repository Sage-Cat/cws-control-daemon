#!/usr/bin/env python3
"""Run unattended, ground-truthed sensing-source hardware-reset trials."""

from __future__ import annotations

import argparse
import random
import time
from typing import Any

from run_campaign import Campaign, add_runtime_arguments, campaign_from_arguments, command


class HardwareResetCampaign(Campaign):
    def build_trials(self) -> list[dict[str, Any]]:
        trials = [
            {
                "trial_id": f"reset-{source_id}-{repetition:02d}",
                "source_id": source_id,
                "fault_type": "esp-hardware-reset",
                "condition": "recommend",
                "repetition": repetition,
            }
            for source_id in sorted(self.nodes)
            for repetition in range(1, int(self.config.get("repetitions_per_node", 5)) + 1)
        ]
        random.Random(int(self.config["seed"])).shuffle(trials)
        for sequence, trial in enumerate(trials, 1):
            trial["randomized_sequence"] = sequence
        return trials

    def reset(self, source_id: str) -> dict[str, Any]:
        result = command(self.nodes[source_id]["reset_argv"], self.timeout, source_id=source_id)
        result["truth_boundary_wall_time_ns"] = result["started_wall_time_ns"]
        if result["status"] != "ok":
            raise RuntimeError(f"hardware reset failed for {source_id}: {result}")
        return result

    def run_trial(self, trial: dict[str, Any]) -> None:
        source_id = str(trial["source_id"])
        self.record("trial-precondition", trial=trial, stability=self.wait_stable())
        self.mirror_marker("start", trial)
        self.record("trial-start", trial=trial, actuation_inhibited=False)
        injected = self.reset(source_id)
        self.record("trial-injected", trial=trial, command=injected)
        detection = self.wait_for_detection(
            source_id, float(self.config.get("detection_timeout_seconds", 15.0))
        )
        self.record("trial-detection-observation", trial=trial, observation=detection)
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
            injection="reviewed hardware-reset adapter command",
        )
        try:
            self.record("campaign-precondition", stability=self.wait_stable())
            for trial in trials:
                self.run_trial(trial)
        except BaseException as exc:
            self.record("campaign-aborted", error=repr(exc))
            raise
        self.record("campaign-complete")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run unattended sensing-source reset trials")
    add_runtime_arguments(parser)
    args = parser.parse_args()
    campaign_from_arguments(
        args,
        campaign_type=HardwareResetCampaign,
        ledger_name="hardware-reset-campaign-ledger.ndjson",
    ).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
