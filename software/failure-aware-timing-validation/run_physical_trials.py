#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time

from run_campaign import add_runtime_arguments, campaign_from_arguments


def main() -> int:
    parser = argparse.ArgumentParser(description="Guide physical sensing-source disconnect trials")
    add_runtime_arguments(parser)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--disconnected-seconds", type=float, default=30.0)
    parser.add_argument("--stable-seconds", type=float, default=45.0)
    args = parser.parse_args()
    campaign = campaign_from_arguments(
        args,
        ledger_name="physical-loss-campaign-ledger.ndjson",
    )
    if args.source_id not in campaign.nodes:
        parser.error("--source-id must name a source declared in --config")
    if campaign.ledger.exists() and campaign.ledger.stat().st_size:
        raise RuntimeError(f"refusing to overwrite {campaign.ledger}")
    trials = [
        {
            "trial_id": f"physical-{index:02d}",
            "randomized_sequence": index,
            "source_id": args.source_id,
            "fault_type": "physical-cable-loss",
            "condition": "physical",
            "repetition": index,
        }
        for index in range(1, args.trials + 1)
    ]
    campaign.record(
        "campaign-start",
        seed=None,
        trial_count=len(trials),
        randomized_trials=trials,
        operator_timestamp_boundary=(
            "The marker is the Enter-key confirmation after the physical action; "
            "interaction latency is retained as an external-validity limitation."
        ),
    )
    try:
        campaign.record("campaign-precondition", stability=campaign.wait_stable())
        for trial in trials:
            campaign.record("trial-precondition", trial=trial, stability=campaign.wait_stable())
            input(
                f"\n[{trial['trial_id']}] Disconnect only {args.source_id}, leave all hardware fixed, "
                "then press Enter: "
            )
            injected_ns = time.time_ns()
            campaign.record(
                "trial-injected",
                trial=trial,
                command={
                    "kind": "operator-cable-disconnect",
                    "status": "operator-confirmed",
                    "ended_wall_time_ns": injected_ns,
                },
            )
            campaign.mirror_marker("start", trial)
            time.sleep(args.disconnected_seconds)
            input(
                f"[{trial['trial_id']}] Reconnect the same {args.source_id} path without moving it, "
                "then press Enter: "
            )
            restored_ns = time.time_ns()
            campaign.record(
                "trial-restore-command",
                trial=trial,
                command={
                    "kind": "operator-cable-reconnect",
                    "status": "operator-confirmed",
                    "ended_wall_time_ns": restored_ns,
                },
            )
            campaign.mirror_marker("restore-command-acked", trial)
            stability = campaign.wait_stable()
            campaign.record("trial-stable", trial=trial, stability=stability)
            campaign.mirror_marker("end", trial)
            time.sleep(args.stable_seconds)
    except BaseException as exc:
        restored = campaign.restore_safe_state()
        campaign.record("campaign-aborted", error=repr(exc), safe_restore=restored)
        raise
    campaign.record("campaign-complete", safe_restore=campaign.restore_safe_state())
    print("Physical failure campaign complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
