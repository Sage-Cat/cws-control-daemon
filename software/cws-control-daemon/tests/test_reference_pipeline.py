from __future__ import annotations

import argparse
import copy
import json
import os
import socket
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from cws_control.cli import run_reference
from cws_control.contracts import ContractError, actuation_transaction, canonical_sha256, contract, source_record, verify_contract
from cws_control.pipeline import (
    DESCRIPTOR_DIMENSIONS, M2_OPERATOR_VERSION, M2_RECOVERED_ANALYZER_SHA256, default_sacm_parameters,
    m1_admit, m2_fuse, m3_select, m4_decide, restore_sacm_state, serialize_sacm_state,
    run_reference_pipeline,
)
from cws_control.reference import serve_reference_socket


def record(source: str, suffix: str = "one", **facts: object) -> dict:
    return {
        "schema_version": "cws-source-record/2", "source_id": source,
        "record_id": f"{source}-{suffix}", "raw": f"synthetic:{source}:{suffix}",
        "session_id": "session-a", "connection_epoch": 1, "source_sequence": 1,
        "ingest_wall_time_ns": 1, "ingest_monotonic_ns": 1,
        "ingest_sequence": 1, "raw_sha256": __import__("hashlib").sha256(f"synthetic:{source}:{suffix}".encode()).hexdigest(),
        "parser_status": "parsed", "parser_reason": "parsed", "device_sequence": 1,
        "boot_epoch": 1, "config_epoch": 1,
        "facts": {"gap": False, "duplicate": False, "reset": False, "reconnect": False,
                  "timing_valid": True, "pair_covered": True, "provenance_valid": True,
                  "stale": False, **facts},
    }


def admitted_m1() -> tuple[dict, dict]:
    return m1_admit(bundle_id="bundle-a", interval_start_ns=0, interval_end_ns=10,
                    expected_sources=["node-a", "node-b"], records=[record("node-a"), record("node-b")],
                    producer_versions={"collector": "test/1"})


def model(width: int, *, multiclass: bool = False) -> dict:
    if multiclass:
        coefficients, intercepts, labels = [[0.0] * width, [0.0] * width, [0.0] * width], [0.0, 1.0, 0.5], ["quiet", "motion", "other"]
    else:
        coefficients, intercepts, labels = [[1.0] + [0.0] * (width - 1)], [0.0], ["quiet", "motion"]
    return contract("cws-m2-model-artifact/1", {
        "operator_version": M2_OPERATOR_VERSION, "provenance": {"kind": "synthetic-fixture"},
        "scaler_mean": [0.0] * width, "scaler_scale": [1.0] * width,
        "class_labels": labels, "coefficients": coefficients, "intercepts": intercepts,
    })


def windows() -> list[dict]:
    output = []
    for node, base in (("node-a", 1.0), ("node-b", 2.0)):
        for sample in (0.0, 2.0):
            value = {"node_id": node, "block_id": "block-a", "qualified": True,
                     "values": [base + sample] + [base] * (DESCRIPTOR_DIMENSIONS - 1), "source_record_hashes": []}
            value["sha256"] = canonical_sha256(value)
            output.append(value)
    return output


def m2_provenance() -> dict:
    return {"paired_path_qualification": {"qualified": True, "protocol_id": "paired-path-v1"},
            "extractor_provenance": {"extractor_version": "synthetic-extractor-v1", "operator_reference_sha256": M2_RECOVERED_ANALYZER_SHA256}}


def candidate(candidate_id: str, nodes: list[str], records: int, quality: float = 0.90, **extra: object) -> dict:
    return {
        "candidate_id": candidate_id, "node_ids": nodes, "retained_records": records,
        "raw_rows": records, "node_count": len(nodes), "quality": quality,
        "quality_binary": quality, "quality_three_state": quality, "qc_passed": True,
        "quality_protocol_id": "quality-protocol", "tiers": {node: "medium" for node in nodes},
        "feasible": True, "normalized_utility": {"quality": quality, "airtime": 0.2, "service": 0.1},
        "active_nodes": nodes, **extra,
    }


def reference_request() -> dict:
    m1 = {
        "bundle_id": "bundle-a",
        "interval_start_ns": 0,
        "interval_end_ns": 10,
        "expected_sources": ["node-a", "node-b"],
        "records": [record("node-a"), record("node-b")],
    }
    bundle, _ = m1_admit(**m1)
    artifact = model(96)
    values = [candidate("anchor-a", ["node-a"], 5)]
    for item in values:
        item.update(
            {
                "evidence_bundle_sha256": bundle["sha256"],
                "model_sha256": artifact["sha256"],
            }
        )
    return {
        "m1": m1,
        "m2": {
            "result_id": "fusion-a",
            "windows": windows(),
            "model_artifact": artifact,
            **m2_provenance(),
        },
        "m3": {
            "result_id": "selection-a",
            "candidates": values,
            "candidate_universe_ids": ["anchor-a"],
            "anchor_id": "node-a",
            "full_reference_id": "anchor-a",
            "quality_protocol_id": "quality-protocol",
        },
    }


class ContractTests(unittest.TestCase):
    def test_canonical_hash_excludes_only_self_hash(self) -> None:
        value = {"b": "тест", "a": {"sha256": "nested"}, "sha256": "wrong"}
        self.assertEqual(canonical_sha256(value), canonical_sha256({"a": {"sha256": "nested"}, "b": "тест"}))
        document = contract("cws-evidence-bundle/1", {"bundle_id": "bundle-a", "reasons": []})
        self.assertEqual(verify_contract(document, versions={"cws-evidence-bundle/1"})["sha256"], document["sha256"])

    def test_v1_is_read_compatible_and_unknown_major_fails_closed(self) -> None:
        legacy = source_record({"schema_version": "cws-source-record/1", "source_id": "node-a", "record_id": "node-a-old", "raw": "legacy", "session_id": "session-a", "connection_epoch": 1, "source_sequence": 2, "ingest_wall_time_ns": 3, "ingest_monotonic_ns": 4})
        self.assertEqual(legacy["source_schema_version"], "cws-source-record/1")
        self.assertIsNone(legacy["device_sequence"])
        self.assertEqual(legacy["session_id"], "session-a")
        self.assertIsNone(legacy["raw_sha256"]); self.assertEqual(len(legacy["raw_sha256_derived"]), 64)
        with self.assertRaises(ContractError):
            source_record({"schema_version": "cws-source-record/3", "source_id": "node-a", "raw": "no"})

    def test_v2_requires_new_facts_and_collector_source_vocabulary(self) -> None:
        parsed = source_record(record("node-a", "record.1"))
        self.assertEqual(parsed["source_id"], "node-a")
        with self.assertRaisesRegex(ContractError, "invalid-source-id"):
            source_record(record("Node.A_1", "record.1"))
        missing = record("node-a"); del missing["parser_status"]
        with self.assertRaisesRegex(ContractError, "missing-parser-status"):
            source_record(missing)
        missing_nullable = record("node-a"); del missing_nullable["boot_epoch"]
        with self.assertRaisesRegex(ContractError, "missing-boot-epoch"):
            source_record(missing_nullable)
        invalid_status = record("node-a"); invalid_status["parser_status"] = "accepted"
        with self.assertRaisesRegex(ContractError, "invalid-parser-status"):
            source_record(invalid_status)
        boolean_sequence = record("node-a"); boolean_sequence["source_sequence"] = True
        with self.assertRaisesRegex(ContractError, "invalid-source-sequence"):
            source_record(boolean_sequence)

    def test_hash_mismatch_fails_closed(self) -> None:
        bundle, verdict = admitted_m1()
        bad = copy.deepcopy(bundle); bad["bundle_id"] = "bundle-b"
        with self.assertRaises(ContractError):
            verify_contract(bad, versions={"cws-evidence-bundle/1"})
        existing = source_record(record("node-a")); existing["facts"]["gap"] = True
        with self.assertRaises(ContractError):
            source_record(existing)
        self.assertTrue(verdict["admitted"])

    def test_actuation_contract_is_data_only_and_fail_closed(self) -> None:
        with self.assertRaises(ContractError):
            actuation_transaction({"transaction_id": "transaction-a"})
        old_flat = {
            "schema_version": "cws-actuation-transaction/1",
            "transaction_id": "transaction-a",
            "target_type": "measurement-rate",
            "terminal_status": "committed",
        }
        with self.assertRaises(ContractError):
            actuation_transaction(old_flat)


class M1Tests(unittest.TestCase):
    def test_duplicate_gap_reset_stale_pair_and_provenance_reject(self) -> None:
        cases = {
            "duplicate": "duplicate-record", "gap": "source-gap", "reset": "source-reset",
            "stale": "source-stale", "pair_covered": "pair-uncovered", "provenance_valid": "provenance-unverified",
        }
        for fact, reason in cases.items():
            with self.subTest(fact=fact):
                value = False if fact in {"pair_covered", "provenance_valid"} else True
                _, verdict = m1_admit(bundle_id="bundle-a", interval_start_ns=0, interval_end_ns=2,
                                       expected_sources=["node-a", "node-b"], records=[record("node-a", **{fact: value}), record("node-b")])
                self.assertFalse(verdict["admitted"]); self.assertIn(reason, verdict["reasons"])

    def test_missing_source_and_reconnect_are_explicit(self) -> None:
        bundle, verdict = m1_admit(bundle_id="bundle-a", interval_start_ns=0, interval_end_ns=2,
                                   expected_sources=["node-a", "node-b"], records=[record("node-a", reconnect=True)])
        self.assertEqual(bundle["record_hashes"], sorted(bundle["record_hashes"]))
        self.assertIn("missing-source", verdict["reasons"]); self.assertIn("source-reconnect", verdict["reasons"])

    def test_all_rows_are_aggregated_and_unexpected_source_rejects(self) -> None:
        _, verdict = m1_admit(bundle_id="bundle-a", interval_start_ns=0, interval_end_ns=2,
                               expected_sources=["node-a", "node-b"],
                               records=[record("node-a", "first"), record("node-a", "later", gap=True, timing_valid=False), record("node-b"), record("node-c")])
        self.assertIn("source-gap", verdict["reasons"])
        self.assertIn("timing-unqualified", verdict["reasons"])
        self.assertIn("unexpected-source", verdict["reasons"])


class M2Tests(unittest.TestCase):
    def test_aggregates_mean_population_std_min_max_and_binary_prediction(self) -> None:
        bundle, verdict = admitted_m1()
        result = m2_fuse(result_id="fusion-a", bundle=bundle, m1_verdict=verdict, windows=windows(), model_artifact=model(96), **m2_provenance())
        self.assertTrue(result["admitted"])
        vector = result["blocks"][0]["vector"]
        self.assertEqual(vector[:4], [2.0, 1.0, 1.0, 3.0])
        self.assertEqual(result["blocks"][0]["prediction"]["label"], "motion")
        self.assertEqual(result["node_order"], ["node-a", "node-b"])
        self.assertEqual(result["ledger"]["scientific_result_eligibility"], "not-established-by-reference-pipeline")

    def test_multiclass_argmax_and_model_hash_mismatch_fail_closed(self) -> None:
        bundle, verdict = admitted_m1()
        result = m2_fuse(result_id="fusion-a", bundle=bundle, m1_verdict=verdict, windows=windows(), model_artifact=model(96, multiclass=True), **m2_provenance())
        self.assertEqual(result["blocks"][0]["prediction"]["label"], "motion")
        bad = model(96); bad["intercepts"] = [9.0]
        rejected = m2_fuse(result_id="fusion-b", bundle=bundle, m1_verdict=verdict, windows=windows(), model_artifact=bad, **m2_provenance())
        self.assertFalse(rejected["admitted"]); self.assertIn("hash-mismatch", rejected["reasons"])
        bad_window = windows(); bad_window[0]["values"][0] = 99.0
        rejected = m2_fuse(result_id="fusion-c", bundle=bundle, m1_verdict=verdict, windows=bad_window, model_artifact=model(96), **m2_provenance())
        self.assertFalse(rejected["admitted"]); self.assertIn("hash-mismatch", rejected["reasons"])
        rejected = m2_fuse(result_id="fusion-d", bundle=bundle, m1_verdict=verdict, windows=windows(), model_artifact=None, **m2_provenance())
        self.assertFalse(rejected["admitted"])
        malformed = model(96); malformed["coefficients"][0][0] = "not-a-number"; malformed["sha256"] = canonical_sha256(malformed)
        rejected = m2_fuse(result_id="fusion-e", bundle=bundle, m1_verdict=verdict, windows=windows(), model_artifact=malformed, **m2_provenance())
        self.assertFalse(rejected["admitted"]); self.assertIn("invalid-model-number", rejected["reasons"])
        malformed = model(96); malformed["class_labels"] = ["same", "same"]; malformed["sha256"] = canonical_sha256(malformed)
        rejected = m2_fuse(result_id="fusion-f", bundle=bundle, m1_verdict=verdict, windows=[None], model_artifact=malformed, **m2_provenance())
        self.assertFalse(rejected["admitted"]); self.assertIn("duplicate-class-label", rejected["reasons"])
        rejected = m2_fuse(result_id="fusion-g", bundle=bundle, m1_verdict=verdict, windows=[None], model_artifact=model(96), **m2_provenance())
        self.assertFalse(rejected["admitted"]); self.assertIn("descriptor-unqualified", rejected["reasons"])


class M3Tests(unittest.TestCase):
    def _select(self, *, solver: str = "exhaustive", **kwargs: object) -> dict:
        bundle, _ = admitted_m1(); artifact = model(96)
        candidates = [candidate("anchor-a", ["node-a"], 5), candidate("full-a-b", ["node-a", "node-b"], 10), candidate("anchor-a-b", ["node-a", "node-b"], 5)]
        for item in candidates:
            item["evidence_bundle_sha256"] = bundle["sha256"]; item["model_sha256"] = artifact["sha256"]
        options = {"record_budget": 20, "node_budget": 3, **kwargs}
        return m3_select(result_id="selection-a", candidates=candidates, candidate_universe_ids=[item["candidate_id"] for item in candidates],
                         anchor_id="node-a", full_reference_id="full-a-b", evidence_bundle_sha256=bundle["sha256"], model_sha256=artifact["sha256"],
                         quality_protocol_id="quality-protocol", solver=solver, **options)

    def test_bm_order_tie_reference_only_qc_service_and_missing_evidence(self) -> None:
        result = self._select()
        self.assertEqual(result["selected_candidate_id"], "anchor-a")
        self.assertEqual(result["ordering"][:2], ["anchor-a", "anchor-a-b"])
        ref_only = self._select(record_budget=10, node_budget=2)
        # The reduced record-5 candidates still qualify; force their QC failure instead below.
        self.assertEqual(ref_only["status"], "selected")
        bundle, _ = m1_admit(bundle_id="bundle-a", interval_start_ns=0, interval_end_ns=10,
                              expected_sources=["node-a", "node-b"], records=[record("node-a"), record("node-b")]); artifact = model(96)
        full = candidate("full-a-b", ["node-a", "node-b"], 10); reduced = candidate("anchor-a", ["node-a"], 5, qc_passed=False, service_impact=None)
        for item in (full, reduced): item.update({"evidence_bundle_sha256": bundle["sha256"], "model_sha256": artifact["sha256"]})
        result = m3_select(result_id="selection-b", candidates=[full, reduced], candidate_universe_ids=["full-a-b", "anchor-a"], anchor_id="node-a", full_reference_id="full-a-b", evidence_bundle_sha256=bundle["sha256"], model_sha256=artifact["sha256"], quality_protocol_id="quality-protocol", service_required=True)
        self.assertEqual(result["status"], "infeasible")  # reference has required service evidence missing too
        self.assertIn("reference-gates-failed", result["reasons"])
        self.assertIn("service-evidence-missing", next(row for row in result["candidate_ledger"] if row["candidate_id"] == "anchor-a")["reasons"])

    def test_no_reduction_is_reference_only_and_qiss_is_seeded_same_gates(self) -> None:
        bundle, _ = admitted_m1(); artifact = model(96)
        full = candidate("full-a-b", ["node-a", "node-b"], 10, .9); bad = candidate("anchor-a", ["node-a"], 5, .6)
        for item in (full, bad): item.update({"evidence_bundle_sha256": bundle["sha256"], "model_sha256": artifact["sha256"]})
        base = dict(result_id="selection-c", candidates=[full, bad], candidate_universe_ids=["full-a-b", "anchor-a"], anchor_id="node-a", full_reference_id="full-a-b", evidence_bundle_sha256=bundle["sha256"], model_sha256=artifact["sha256"], quality_protocol_id="quality-protocol")
        exhaustive = m3_select(**base); self.assertEqual(exhaustive["status"], "reference-only")
        one = m3_select(**base, solver="qiss", seed=7, search_budget=2); two = m3_select(**base, solver="qiss", seed=7, search_budget=2)
        self.assertEqual(one["visited"], two["visited"]); self.assertEqual(one["status"], "reference-only")
        self.assertIn("active_probabilities", one["qiss_state"])
        updated = self._select(solver="qiss", seed=7, search_budget=3)
        trace = updated["qiss_state"]["update_trace"]
        self.assertGreaterEqual(len(trace), 2)
        changed = trace[0]
        self.assertTrue(changed["updated"])
        self.assertNotEqual(changed["active_probabilities_before"], changed["active_probabilities_after"])
        self.assertEqual(trace[1]["active_probabilities_before"], changed["active_probabilities_after"])

    def test_dual_quality_gates_and_duplicate_universe_reject(self) -> None:
        bundle, _ = admitted_m1(); artifact = model(96)
        full = candidate("full-a-b", ["node-a", "node-b"], 10, .9)
        reduced = candidate("anchor-a", ["node-a"], 5, .9); reduced["quality_three_state"] = .74
        for item in (full, reduced): item.update({"evidence_bundle_sha256": bundle["sha256"], "model_sha256": artifact["sha256"]})
        base = dict(result_id="selection-dual", candidates=[full, reduced], candidate_universe_ids=["full-a-b", "anchor-a"], anchor_id="node-a", full_reference_id="full-a-b", evidence_bundle_sha256=bundle["sha256"], model_sha256=artifact["sha256"], quality_protocol_id="quality-protocol")
        result = m3_select(**base)
        row = next(row for row in result["candidate_ledger"] if row["candidate_id"] == "anchor-a")
        self.assertIn("three-state-quality-margin-failed", row["reasons"])
        duplicated = m3_select(**{**base, "candidate_universe_ids": ["full-a-b", "anchor-a", "anchor-a"]})
        self.assertFalse(duplicated["admitted"]); self.assertIn("duplicate-candidate-universe-id", duplicated["reasons"])
        malformed = m3_select(**{**base, "candidates": [None]})
        self.assertFalse(malformed["admitted"]); self.assertIn("invalid-m3-input", malformed["reasons"])


class M4AndPipelineTests(unittest.TestCase):
    def _m3_and_candidate(self, *, feasible: bool = True, quality: float = .9) -> tuple[dict, dict]:
        bundle, _ = admitted_m1(); artifact = model(96)
        value = candidate("anchor-a", ["node-a"], 5, quality, feasible=feasible)
        value.update({"evidence_bundle_sha256": bundle["sha256"], "model_sha256": artifact["sha256"]})
        m3 = m3_select(result_id="selection-a", candidates=[value], candidate_universe_ids=["anchor-a"], anchor_id="node-a", full_reference_id="anchor-a", evidence_bundle_sha256=bundle["sha256"], model_sha256=artifact["sha256"], quality_protocol_id="quality-protocol")
        return m3, value

    def test_sacm_replace_hold_fallback_and_restart(self) -> None:
        m3, value = self._m3_and_candidate()
        decision, state = m4_decide(decision_id="decision-a", m3_result=m3, candidate=value)
        self.assertEqual(decision["action"], "replace")
        held, restored = m4_decide(decision_id="decision-b", m3_result=m3, candidate=value, state=restore_sacm_state(state))
        self.assertEqual(held["action"], "hold"); self.assertEqual(held["decision_epoch"], 2)
        bad_m3, bad = self._m3_and_candidate(feasible=False)
        fallback, _ = m4_decide(decision_id="decision-c", m3_result=bad_m3, candidate=bad)
        self.assertEqual(fallback["action"], "fallback")
        self.assertEqual(decision["parameters"], default_sacm_parameters())
        tampered = copy.deepcopy(value); tampered["normalized_utility"]["quality"] = .1
        rejected, preserved = m4_decide(decision_id="decision-d", m3_result=m3, candidate=tampered, state=state)
        self.assertFalse(rejected["admitted"]); self.assertIn("selected-candidate-hash-mismatch", rejected["reasons"])
        self.assertEqual(preserved["sha256"], state["sha256"])
        invalid, preserved = m4_decide(decision_id="decision-e", m3_result=m3, candidate=value, state=state, parameters={"alpha_node": float("nan")})
        self.assertFalse(invalid["admitted"]); self.assertEqual(preserved["decision_epoch"], state["decision_epoch"])
        invalid, _ = m4_decide(decision_id="decision-f", m3_result=m3, candidate=value, state={})
        self.assertFalse(invalid["admitted"]); self.assertIn("invalid-sacm-state", invalid["reasons"])
        with self.assertRaises(ContractError):
            serialize_sacm_state({"decision_epoch": -1, "incumbent": {}})

    def test_full_pipeline_is_hash_linked_and_stops_on_m1_rejection(self) -> None:
        bundle, _ = m1_admit(bundle_id="bundle-a", interval_start_ns=0, interval_end_ns=10,
                              expected_sources=["node-a", "node-b"], records=[record("node-a"), record("node-b")])
        artifact = model(96)
        values = [candidate("anchor-a", ["node-a"], 5)]
        for item in values: item.update({"evidence_bundle_sha256": bundle["sha256"], "model_sha256": artifact["sha256"]})
        good = run_reference_pipeline(
            m1={"bundle_id": "bundle-a", "interval_start_ns": 0, "interval_end_ns": 10, "expected_sources": ["node-a", "node-b"], "records": [record("node-a"), record("node-b")]},
            m2={"result_id": "fusion-a", "windows": windows(), "model_artifact": artifact, **m2_provenance()},
            m3={"result_id": "selection-a", "candidates": values, "candidate_universe_ids": ["anchor-a"], "anchor_id": "node-a", "full_reference_id": "anchor-a", "quality_protocol_id": "quality-protocol"},
        )
        self.assertEqual(good["status"], "accepted")
        self.assertEqual([row["stage"] for row in good["ledger"]], ["m1-bundle", "m1", "m2", "m3", "m4"])
        rejected = run_reference_pipeline(
            m1={"bundle_id": "bundle-a", "interval_start_ns": 0, "interval_end_ns": 10, "expected_sources": ["node-a", "node-b"], "records": [record("node-a", gap=True)]},
            m2={"result_id": "fusion-a", "windows": windows(), "model_artifact": artifact, **m2_provenance()}, m3={"result_id": "selection-a", "candidates": [], "candidate_universe_ids": [], "anchor_id": "node-a", "full_reference_id": "full-a", "quality_protocol_id": "quality-protocol"},
        )
        self.assertEqual(rejected["failed_stage"], "m1"); self.assertEqual(len(rejected["ledger"]), 2)


class ReferenceUnixServiceTests(unittest.TestCase):
    def start_service(self, socket_path: Path, **kwargs: object) -> tuple[threading.Thread, list[BaseException]]:
        ready = threading.Event()
        errors: list[BaseException] = []

        def target() -> None:
            try:
                serve_reference_socket(socket_path, ready=ready.set, **kwargs)
            except BaseException as exc:  # surfaced deterministically in the test thread
                errors.append(exc)

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        if not ready.wait(2.0):
            thread.join(0.1)
            self.fail(f"reference service did not become ready: {errors!r}")
        return thread, errors

    def exchange(self, socket_path: Path, encoded: bytes) -> dict:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            client.sendall(encoded)
            client.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                chunk = client.recv(65_536)
                if not chunk:
                    break
                chunks.append(chunk)
        return json.loads(b"".join(chunks))

    def finish(self, thread: threading.Thread, errors: list[BaseException]) -> None:
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        if errors:
            raise errors[0]

    def test_one_shot_and_unix_service_have_exact_output_and_state_parity(self) -> None:
        payload = reference_request()
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "input.json"
            output_path = root / "one-shot.json"
            one_shot_state = root / "one-shot-state.json"
            service_state = root / "service-state.json"
            socket_path = root / "reference.sock"
            input_path.write_bytes(encoded)
            run_reference(
                argparse.Namespace(
                    input=input_path,
                    output=output_path,
                    state_input=None,
                    state_output=one_shot_state,
                )
            )
            thread, errors = self.start_service(
                socket_path,
                state_path=service_state,
                max_requests=1,
            )
            with mock.patch("cws_control.engine.subprocess.run") as actuator:
                service_output = self.exchange(socket_path, encoded)
                actuator.assert_not_called()
            self.finish(thread, errors)
            one_shot_output = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(service_output, one_shot_output)
            self.assertEqual(
                json.loads(service_state.read_text(encoding="utf-8")),
                json.loads(one_shot_state.read_text(encoding="utf-8")),
            )
            self.assertFalse(os.path.lexists(socket_path))

    def test_state_and_derived_history_persist_across_requests_and_restart(self) -> None:
        payload = reference_request()
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            socket_path = root / "reference.sock"
            state_path = root / "state.json"
            history_path = root / "history.ndjson"
            thread, errors = self.start_service(
                socket_path,
                state_path=state_path,
                history_path=history_path,
                max_requests=2,
            )
            first = self.exchange(socket_path, encoded)
            second = self.exchange(socket_path, encoded)
            self.finish(thread, errors)
            self.assertEqual(first["result"]["decision_epoch"], 1)
            self.assertEqual(second["result"]["decision_epoch"], 2)

            thread, errors = self.start_service(
                socket_path,
                state_path=state_path,
                history_path=history_path,
                max_requests=1,
            )
            third = self.exchange(socket_path, encoded)
            self.finish(thread, errors)
            self.assertEqual(third["result"]["decision_epoch"], 3)
            stored = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(stored["decision_epoch"], 3)
            history = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(history, [first, second, third])
            self.assertNotIn("private_note", history_path.read_text(encoding="utf-8"))
            self.assertNotIn('"raw"', history_path.read_text(encoding="utf-8"))

    def test_malformed_schema_oversize_and_duplicate_state_fail_without_mutation(self) -> None:
        payload = reference_request()
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        initial = run_reference_pipeline(**payload)["state"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            socket_path = root / "reference.sock"
            state_path = root / "state.json"
            history_path = root / "history.ndjson"
            state_path.write_text(json.dumps(initial, sort_keys=True), encoding="utf-8")
            before = state_path.read_bytes()
            maximum = 1_048_576
            thread, errors = self.start_service(
                socket_path,
                state_path=state_path,
                history_path=history_path,
                max_request_bytes=maximum,
                max_requests=5,
            )
            cases = [
                (b"{", "invalid-json"),
                (
                    json.dumps({**payload, "private_note": "must-not-be-copied"}).encode("utf-8"),
                    "unexpected-request-field",
                ),
                (
                    json.dumps({**payload, "state": initial}).encode("utf-8"),
                    "duplicate-state-input",
                ),
                (b"x" * (maximum + 1), "request-too-large"),
            ]
            for request, reason in cases:
                with self.subTest(reason=reason):
                    response = self.exchange(socket_path, request)
                    self.assertEqual(response["schema_version"], "cws-reference-service-error/1")
                    self.assertEqual(response["reason"], reason)
                    verify_contract(response, versions={"cws-reference-service-error/1"})
                    self.assertEqual(state_path.read_bytes(), before)
            accepted = self.exchange(socket_path, encoded)
            self.finish(thread, errors)
            self.assertEqual(accepted["status"], "accepted")
            history = history_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(history), 1)
            self.assertNotIn("private_note", history[0])

    def test_socket_is_private_refuses_collisions_and_does_not_unlink_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            collision = root / "collision.sock"
            collision.write_text("operator-owned", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "socket-path-exists"):
                serve_reference_socket(collision, max_requests=1)
            self.assertEqual(collision.read_text(encoding="utf-8"), "operator-owned")

            socket_path = root / "reference.sock"
            thread, errors = self.start_service(socket_path, max_requests=1)
            self.assertEqual(stat.S_IMODE(socket_path.lstat().st_mode), 0o600)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(socket_path))
                socket_path.unlink()
                socket_path.write_text("replacement", encoding="utf-8")
                client.sendall(b"{}")
                client.shutdown(socket.SHUT_WR)
                while client.recv(65_536):
                    pass
            self.finish(thread, errors)
            self.assertEqual(socket_path.read_text(encoding="utf-8"), "replacement")


if __name__ == "__main__":
    unittest.main()
