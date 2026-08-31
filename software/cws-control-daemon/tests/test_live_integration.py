from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cws_control.cli import run_reference
from cws_control.contracts import (
    ContractError,
    actuation_transaction,
    canonical_sha256,
    verify_contract,
)
from cws_control.engine import CommandActuator, ControlEngine
from cws_control.parser import ParseError, parse_envelope
from cws_control.pipeline import default_sacm_parameters, serialize_sacm_state


def raw_csi(sequence: int, timestamp_us: int) -> str:
    values = [3, 4, 5, 12]
    return (
        f'CSI_DATA,{sequence},aa:bb:cc:dd:ee:ff,-45,1,1,0,0,0,0,0,0,0,0,-95,0,6,0,'
        f'{timestamp_us},0,128,0,{len(values)},0,"{values}"'
    )


def source_record(
    source_sequence: int,
    raw: str,
    *,
    session_id: str = "session-a",
    connection_epoch: int = 1,
) -> dict:
    return {
        "schema_version": "cws-source-record/2",
        "source_id": "node-a",
        "session_id": session_id,
        "connection_epoch": connection_epoch,
        "source_sequence": source_sequence,
        "ingest_wall_time_ns": source_sequence * 1_000_000,
        "ingest_monotonic_ns": source_sequence * 1_000_000,
        "raw": raw,
        "ingest_sequence": source_sequence,
        "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "parser_status": "parsed",
        "parser_reason": "csi-data",
        "device_sequence": source_sequence,
        "boot_epoch": 1,
        "config_epoch": 1,
    }


def engine_config(*, reorder_window_records: int = 2) -> dict:
    return {
        "schema_version": 1,
        "nodes": {"node-a": {}},
        "mode": "recommend",
        "minimum_frames": 1,
        "reorder_window_records": reorder_window_records,
        "ordering_event_limit": 16,
        "ordering_context_history": 8,
    }


def committed_result(source_id: str = "node-a", hz: int = 20) -> dict:
    phase_ids = {
        "state-before": "txn-a-s0",
        "prepare": "txn-a-p",
        "apply": "txn-a-a",
        "verify": "txn-a-v",
    }
    applied = "3" * 64
    requested = canonical_sha256(
        {
            "schema_version": "cws-firmware-state-request/1",
            "source_id": source_id,
            "transaction_id": "txn-a",
            "requested_ping_hz": hz,
        }
    )
    facts = {
        "requested_state_sha256": requested,
        "epoch_bound_requested_state_sha256": "4" * 64,
        "pre_state_sha256": "2" * 64,
        "applied_state_sha256": applied,
        "restored_state_sha256": None,
        "apply_observed_state_sha256": applied,
        "restore_observed_state_sha256": None,
        "phase_command_ids": phase_ids,
        "phase_deadline_wall_time_ns": {phase: index + 1 for index, phase in enumerate(phase_ids)},
        "correlated_replies": [
            {"phase": phase, "command_id": command_id, "reply_sha256": f"{index + 5:x}" * 64}
            for index, (phase, command_id) in enumerate(phase_ids.items())
        ],
        "postcondition": {
            "verified": True,
            "reason": "postcondition-verified",
            "state_sha256": applied,
        },
        "rollback": {"requested": False},
    }
    return {
        "schema_version": "cws-actuation-transaction/1",
        "command_id": "txn-a",
        "transaction_id": "txn-a",
        "source_id": source_id,
        "run_id": "run-a",
        "hz": hz,
        "status": "applied",
        "transaction_status": "committed",
        "reason": "postcondition-verified",
        "responded_wall_time_ns": 10,
        "terminal_facts": facts,
        "terminal_facts_sha256": canonical_sha256(facts),
    }


def recovered_committed_result(source_id: str = "node-a", hz: int = 20) -> dict:
    value = committed_result(source_id, hz)
    facts = value["terminal_facts"]
    del facts["phase_command_ids"]["verify"]
    del facts["phase_deadline_wall_time_ns"]["verify"]
    facts["correlated_replies"] = [
        reply
        for reply in facts["correlated_replies"]
        if reply["phase"] not in {"apply", "verify"}
    ]
    facts["phase_command_ids"].update(
        {"query-1": "txn-a-q1", "recover-state": "txn-a-r"}
    )
    facts["phase_deadline_wall_time_ns"].update(
        {"query-1": 5, "recover-state": 6}
    )
    facts["correlated_replies"].extend(
        [
            {"phase": "recover-state", "command_id": "txn-a-r", "reply_sha256": "a" * 64},
        ]
    )
    facts["apply_observed_state_sha256"] = None
    facts["postcondition"]["reason"] = "postcondition-recovered"
    value["reason"] = "postcondition-recovered"
    value["terminal_facts_sha256"] = canonical_sha256(facts)
    return value


def decision_bound_result(*, recovered: bool = False) -> dict:
    decision_sha256 = "d" * 64
    value = recovered_committed_result() if recovered else committed_result()
    value["schema_version"] = "cws-actuation-transaction/2"
    value["decision_sha256"] = decision_sha256
    facts = value["terminal_facts"]
    facts["decision_sha256"] = decision_sha256
    facts["requested_state_sha256"] = canonical_sha256(
        {
            "schema_version": "cws-firmware-state-request/2",
            "source_id": value["source_id"],
            "transaction_id": value["transaction_id"],
            "requested_ping_hz": value["hz"],
            "decision_sha256": decision_sha256,
        }
    )
    value["terminal_facts_sha256"] = canonical_sha256(facts)
    return value


class SourceRecordTests(unittest.TestCase):
    def test_v2_is_strict_and_retains_nullable_epoch_facts(self) -> None:
        value = source_record(1, raw_csi(1, 1_000))
        value.update({"device_sequence": None, "boot_epoch": None, "config_epoch": None})
        parsed = parse_envelope(value)
        self.assertEqual(parsed.schema_version, "cws-source-record/2")
        self.assertEqual(parsed.raw_sha256, value["raw_sha256"])
        self.assertIsNone(parsed.boot_epoch)

        bad_hash = dict(value, raw_sha256="0" * 64)
        with self.assertRaisesRegex(ParseError, "raw-hash-mismatch"):
            parse_envelope(bad_hash)
        with self.assertRaisesRegex(ParseError, "unknown-source-record-major"):
            parse_envelope(dict(value, schema_version="cws-source-record/3"))
        missing = dict(value)
        del missing["parser_reason"]
        with self.assertRaisesRegex(ParseError, "missing-v2-field"):
            parse_envelope(missing)

    def test_v1_remains_read_compatible(self) -> None:
        value = source_record(1, raw_csi(1, 1_000))
        for field in (
            "schema_version",
            "ingest_sequence",
            "raw_sha256",
            "parser_status",
            "parser_reason",
            "device_sequence",
            "boot_epoch",
            "config_epoch",
        ):
            value.pop(field)
        parsed = parse_envelope(value)
        self.assertEqual(parsed.schema_version, "cws-source-record/1")
        self.assertIsNone(parsed.ingest_sequence)


class OrderingIntegrationTests(unittest.TestCase):
    def test_out_of_order_records_reach_timing_in_source_order(self) -> None:
        engine = ControlEngine(engine_config(reorder_window_records=3))
        self.assertTrue(engine.ingest(source_record(1, raw_csi(1, 1_000))))
        self.assertTrue(engine.ingest(source_record(3, raw_csi(3, 3_000))))
        self.assertEqual([frame.sequence for frame in engine.states["node-a"].frames], [1])
        self.assertTrue(engine.ingest(source_record(2, raw_csi(2, 2_000))))
        self.assertEqual(
            [frame.sequence for frame in engine.states["node-a"].frames],
            [1, 2, 3],
        )
        self.assertEqual(
            [timestamp for timestamp, _ in engine.states["node-a"].timing.points],
            [1_000, 2_000, 3_000],
        )
        ledger = engine.decide(4_000_000)["ordering"]["node-a"]
        self.assertEqual(ledger["counts"]["buffered"], 1)
        self.assertEqual(ledger["buffered_current"], 0)

    def test_duplicate_forced_gap_and_late_record_are_counted(self) -> None:
        engine = ControlEngine(engine_config(reorder_window_records=2))
        engine.ingest(source_record(1, raw_csi(1, 1_000)))
        self.assertFalse(engine.ingest(source_record(1, raw_csi(1, 1_000))))
        engine.ingest(source_record(4, raw_csi(4, 4_000)))
        engine.ingest(source_record(5, raw_csi(5, 5_000)))
        self.assertFalse(engine.ingest(source_record(2, raw_csi(2, 2_000))))
        ledger = engine.decide(6_000_000)["ordering"]["node-a"]
        self.assertEqual(ledger["counts"]["duplicates"], 1)
        self.assertEqual(ledger["counts"]["gaps"], 1)
        self.assertEqual(ledger["counts"]["missing_records"], 2)
        self.assertEqual(ledger["counts"]["late"], 1)
        self.assertEqual(engine.states["node-a"].timing.points[0][0], 4_000)
        self.assertIn("gap", [row["event"] for row in ledger["recent_events"]])

    def test_reconnect_reset_flush_and_stale_old_context(self) -> None:
        engine = ControlEngine(engine_config(reorder_window_records=3))
        engine.ingest(source_record(1, raw_csi(1, 1_000)))
        engine.ingest(source_record(3, raw_csi(3, 3_000)))
        engine.ingest(
            source_record(1, raw_csi(1, 10_000), connection_epoch=2)
        )
        self.assertFalse(
            engine.ingest(source_record(2, raw_csi(2, 2_000), connection_epoch=1))
        )
        engine.ingest(
            source_record(1, raw_csi(1, 20_000), session_id="session-b", connection_epoch=1)
        )
        self.assertFalse(
            engine.ingest(source_record(2, raw_csi(2, 11_000), connection_epoch=2))
        )
        ledger = engine.decide(30_000_000)["ordering"]["node-a"]
        self.assertEqual(ledger["counts"]["reconnects"], 1)
        self.assertEqual(ledger["counts"]["resets"], 1)
        self.assertEqual(ledger["counts"]["flushed_buffer_records"], 1)
        self.assertEqual(ledger["counts"]["late"], 2)
        events = [row["event"] for row in ledger["recent_events"]]
        self.assertIn("reconnect", events)
        self.assertIn("reset", events)
        self.assertIn("stale-context", events)


class CommandActuatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.actuator = CommandActuator({"node-a": {"argv": ["collector", "rate", "{source_id}", "{hz}"]}})

    def completed(self, stdout: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["collector"], returncode, stdout=stdout, stderr="")

    @mock.patch("cws_control.engine.subprocess.run")
    def test_valid_collector_terminal_result_is_the_only_success(self, run: mock.Mock) -> None:
        value = committed_result()
        self.assertEqual(
            actuation_transaction(value, expected_source_id="node-a", expected_hz=20),
            value,
        )
        run.return_value = self.completed(json.dumps(value))
        result = self.actuator.apply("node-a", 20)
        self.assertEqual(result["status"], "applied")
        self.assertTrue(result["verified_committed"])
        self.assertEqual(result["transaction_id"], "txn-a")
        self.assertIsNone(result["decision_sha256"])
        run.assert_called_once_with(
            ["collector", "rate", "node-a", "20"],
            capture_output=True,
            text=True,
            timeout=10.0,
            check=False,
        )

    @mock.patch("cws_control.engine.subprocess.run")
    def test_recovered_collector_commit_is_verified(self, run: mock.Mock) -> None:
        value = recovered_committed_result()
        run.return_value = self.completed(json.dumps(value))
        result = self.actuator.apply("node-a", 20)
        self.assertEqual(result["status"], "applied")
        self.assertTrue(result["verified_committed"])
        self.assertEqual(actuation_transaction(value)["reason"], "postcondition-recovered")

    @mock.patch("cws_control.engine.subprocess.run")
    def test_v2_normal_and_recovered_commits_bind_expected_decision(
        self, run: mock.Mock
    ) -> None:
        decision_sha256 = "d" * 64
        actuator = CommandActuator(
            {
                "node-a": {
                    "argv": [
                        "collector",
                        "rate",
                        "{source_id}",
                        "{hz}",
                        "--decision-sha256",
                        "{decision_sha256}",
                        "--transaction-id",
                        "{transaction_id}",
                    ]
                }
            }
        )
        for recovered in (False, True):
            with self.subTest(recovered=recovered):
                value = decision_bound_result(recovered=recovered)
                self.assertEqual(
                    actuation_transaction(
                        value,
                        expected_source_id="node-a",
                        expected_hz=20,
                        expected_decision_sha256=decision_sha256,
                    ),
                    value,
                )
                run.return_value = self.completed(json.dumps(value))
                result = actuator.apply(
                    "node-a",
                    20,
                    decision_sha256=decision_sha256,
                    transaction_id="txn-a",
                )
                self.assertEqual(result["status"], "applied")
                self.assertEqual(result["schema_version"], "cws-actuation-transaction/2")
                self.assertEqual(result["decision_sha256"], decision_sha256)
                self.assertEqual(
                    run.call_args.args[0][-4:],
                    ["--decision-sha256", decision_sha256, "--transaction-id", "txn-a"],
                )

    @mock.patch("cws_control.engine.subprocess.run")
    def test_v2_rejects_downgrade_and_decision_correlations(self, run: mock.Mock) -> None:
        decision_sha256 = "d" * 64
        run.return_value = self.completed(json.dumps(committed_result()))
        downgraded = self.actuator.apply(
            "node-a", 20, decision_sha256=decision_sha256, transaction_id="txn-a"
        )
        self.assertEqual(downgraded["reason"], "decision-link-required")

        run.return_value = self.completed(json.dumps(decision_bound_result()))
        missing_context = self.actuator.apply("node-a", 20)
        self.assertEqual(missing_context["reason"], "decision-context-required")

        wrong_expected = decision_bound_result()
        run.return_value = self.completed(json.dumps(wrong_expected))
        mismatch = self.actuator.apply(
            "node-a", 20, decision_sha256="e" * 64, transaction_id="txn-a"
        )
        self.assertEqual(mismatch["reason"], "expected-decision-mismatch")

        nested = decision_bound_result()
        nested["terminal_facts"]["decision_sha256"] = "e" * 64
        nested["terminal_facts_sha256"] = canonical_sha256(nested["terminal_facts"])
        with self.assertRaisesRegex(ContractError, "decision-link-mismatch"):
            actuation_transaction(nested)

        missing_top = decision_bound_result()
        del missing_top["decision_sha256"]
        with self.assertRaisesRegex(ContractError, "missing-decision-sha256"):
            actuation_transaction(missing_top)

        missing_nested = decision_bound_result()
        del missing_nested["terminal_facts"]["decision_sha256"]
        with self.assertRaisesRegex(ContractError, "missing-terminal-decision-sha256"):
            actuation_transaction(missing_nested)

        wrong_request = decision_bound_result()
        wrong_request["terminal_facts"]["requested_state_sha256"] = canonical_sha256(
            {
                "schema_version": "cws-firmware-state-request/1",
                "source_id": "node-a",
                "transaction_id": "txn-a",
                "requested_ping_hz": 20,
            }
        )
        wrong_request["terminal_facts_sha256"] = canonical_sha256(
            wrong_request["terminal_facts"]
        )
        with self.assertRaisesRegex(ContractError, "requested-state-correlation-mismatch"):
            actuation_transaction(wrong_request)

        returned_tx = decision_bound_result()
        run.return_value = self.completed(json.dumps(returned_tx))
        mismatch = self.actuator.apply(
            "node-a", 20, decision_sha256=decision_sha256, transaction_id="txn-other"
        )
        self.assertEqual(mismatch["reason"], "transaction-id-mismatch")

        run.reset_mock()
        invalid = self.actuator.apply(
            "node-a", 20, decision_sha256="bad", transaction_id="txn-a"
        )
        self.assertEqual(invalid["status"], "invalid-actuation-context")
        run.assert_not_called()

    @mock.patch("cws_control.engine.subprocess.run")
    def test_zero_exit_non_json_is_failure(self, run: mock.Mock) -> None:
        run.return_value = self.completed("not-json")
        self.assertEqual(self.actuator.apply("node-a", 20)["reason"], "invalid-json-result")

    @mock.patch("cws_control.engine.subprocess.run")
    def test_source_rate_transaction_and_terminal_mismatches_fail(self, run: mock.Mock) -> None:
        cases = []
        wrong_source = committed_result(source_id="node-b")
        cases.append((wrong_source, "source-mismatch"))
        wrong_rate = committed_result(hz=10)
        cases.append((wrong_rate, "rate-mismatch"))
        wrong_transaction = committed_result()
        wrong_transaction["command_id"] = "other"
        cases.append((wrong_transaction, "transaction-correlation-mismatch"))
        wrong_terminal = committed_result()
        wrong_terminal["transaction_status"] = "rolled-back"
        cases.append((wrong_terminal, "terminal-status-mismatch"))
        wrong_hash = committed_result()
        wrong_hash["terminal_facts_sha256"] = "0" * 64
        cases.append((wrong_hash, "terminal-facts-hash-mismatch"))
        for value, expected in cases:
            with self.subTest(expected=expected):
                run.return_value = self.completed(json.dumps(value))
                self.assertEqual(self.actuator.apply("node-a", 20)["reason"], expected)

    @mock.patch("cws_control.engine.subprocess.run")
    def test_timeout_is_explicit(self, run: mock.Mock) -> None:
        run.side_effect = subprocess.TimeoutExpired(["collector"], 10.0)
        result = self.actuator.apply("node-a", 20)
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(result["reason"], "actuator-timeout")

    @mock.patch("cws_control.engine.subprocess.run")
    def test_phase_reply_deadline_and_postcondition_evidence_are_required(
        self, run: mock.Mock
    ) -> None:
        cases: list[tuple[dict, str]] = []
        no_deadline = committed_result()
        del no_deadline["terminal_facts"]["phase_deadline_wall_time_ns"]["verify"]
        cases.append((no_deadline, "phase-deadline-mismatch"))
        wrong_reply = committed_result()
        wrong_reply["terminal_facts"]["correlated_replies"][0]["command_id"] = "other"
        cases.append((wrong_reply, "reply-phase-correlation-mismatch"))
        unverified = committed_result()
        unverified["terminal_facts"]["postcondition"]["verified"] = False
        cases.append((unverified, "postcondition-unverified"))
        for value, expected in cases:
            with self.subTest(expected=expected):
                value["terminal_facts_sha256"] = canonical_sha256(value["terminal_facts"])
                run.return_value = self.completed(json.dumps(value))
                self.assertEqual(self.actuator.apply("node-a", 20)["reason"], expected)

    def test_cross_mean_contract_rejects_old_flat_and_broken_recovery(self) -> None:
        old_flat = {
            "schema_version": "cws-actuation-transaction/1",
            "transaction_id": "txn-a",
            "target_type": "measurement-rate",
            "terminal_status": "committed",
        }
        with self.assertRaises(ContractError):
            actuation_transaction(old_flat)
        broken = recovered_committed_result()
        broken["terminal_facts"]["correlated_replies"] = [
            reply
            for reply in broken["terminal_facts"]["correlated_replies"]
            if reply["phase"] != "recover-state"
        ]
        broken["terminal_facts_sha256"] = canonical_sha256(broken["terminal_facts"])
        with self.assertRaisesRegex(ContractError, "incomplete-recovery-replies"):
            actuation_transaction(broken)

        fabricated_query_reply = recovered_committed_result()
        fabricated_query_reply["terminal_facts"]["correlated_replies"].append(
            {"phase": "query", "command_id": "txn-a-q1", "reply_sha256": "9" * 64}
        )
        fabricated_query_reply["terminal_facts_sha256"] = canonical_sha256(
            fabricated_query_reply["terminal_facts"]
        )
        with self.assertRaisesRegex(ContractError, "invalid-recovered-reply-evidence"):
            actuation_transaction(fabricated_query_reply)


class ReferenceCliTests(unittest.TestCase):
    @mock.patch("cws_control.reference.run_reference_pipeline")
    def test_cli_calls_pure_pipeline_and_atomically_persists_verified_state(
        self, pipeline: mock.Mock
    ) -> None:
        state = serialize_sacm_state(
            {
                "component_version": "sacm-replay-reference/1",
                "decision_epoch": 1,
                "incumbent": {
                    "candidate_id": "fallback",
                    "feasible": False,
                    "normalized_utility": {"quality": 0.0, "airtime": 0.0, "service": 0.0},
                    "active_nodes": [],
                    "tiers": {},
                },
                "parameters": default_sacm_parameters(),
            }
        )
        pipeline.return_value = {
            "status": "accepted",
            "failed_stage": None,
            "ledger": [{"stage": "m4", "sha256": "9" * 64}],
            "result": {"schema_version": "cws-m4-decision/1", "sha256": "8" * 64},
            "state": state,
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "input.json"
            state_input = root / "state-input.json"
            output_path = root / "output.json"
            state_output = root / "state-output.json"
            input_path.write_text(
                json.dumps({"m1": {}, "m2": {}, "m3": {}}),
                encoding="utf-8",
            )
            state_input.write_text(json.dumps(state), encoding="utf-8")
            result = run_reference(
                argparse.Namespace(
                    input=input_path,
                    output=output_path,
                    state_input=state_input,
                    state_output=state_output,
                )
            )
            output = json.loads(output_path.read_text(encoding="utf-8"))
            restored = json.loads(state_output.read_text(encoding="utf-8"))
        self.assertEqual(result, 0)
        self.assertNotIn("m1", output)
        self.assertEqual(
            verify_contract(output, versions={"cws-reference-pipeline-run/1"})["sha256"],
            output["sha256"],
        )
        self.assertEqual(restored["sha256"], state["sha256"])
        self.assertEqual(pipeline.call_count, 1)
        self.assertEqual(pipeline.call_args.kwargs["state"]["sha256"], state["sha256"])


if __name__ == "__main__":
    unittest.main()
