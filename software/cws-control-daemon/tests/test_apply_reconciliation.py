from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cws_control.engine import ControlEngine
from cws_control.models import Configuration
from cws_control.policy import AqmcParameters, configuration_utility, transition_metrics
from cws_control.reporting import summarize_decisions
from test_controller import evidence


class FakeActuator:
    """In-memory rates only; failures may occur after the simulated mutation."""

    def __init__(self):
        self.rates = {"a": 0, "b": 0}
        self.calls = []
        self.failure = None

    def apply(self, source_id, hz):
        self.calls.append((source_id, hz))
        self.rates[source_id] = hz
        if self.failure is not None and source_id == "b":
            status = self.failure
            self.failure = None
            if status == "unverified":
                return {"source_id": source_id, "hz": hz, "status": "applied"}
            if status == "wrong-source":
                result = self.commit(source_id, hz)
                result["source_id"] = "a"
                return result
            return {"source_id": source_id, "hz": hz, "status": status}
        return self.commit(source_id, hz)

    @staticmethod
    def commit(source_id, hz):
        return {
            "source_id": source_id, "hz": hz, "status": "applied",
            "schema_version": "cws-actuation-transaction/1",
            "transaction_status": "committed", "verified_committed": True,
            "transaction_id": "fixture-transaction", "terminal_facts_sha256": "fixture-facts",
        }


class ApplyReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.actuator = FakeActuator()
        self.engine = ControlEngine({
            "nodes": {"a": {}, "b": {}}, "mode": "apply",
            "evidence_transport": "loopback-authenticated-tunnel/1",
            "aqmc": {"rate_profile": "hardware-40hz"},
            "sacm": {"quality_weight": 0.0, "airtime_weight": 1.0, "qos_weight": 0.0,
                     "alpha_rate": 0.0001},
        }, self.actuator)
        self.items = {source: evidence(source, 1.2, "high") for source in ("a", "b")}
        patcher = mock.patch.object(self.engine, "_evidence", side_effect=lambda source, now: self.items[source])
        patcher.start()
        self.addCleanup(patcher.stop)
        self.now = 1000000000

    def decide(self):
        self.now += 100000000
        return self.engine.decide(self.now)

    def tiers(self, tier, unavailable=None):
        self.items = {source: evidence(source, 1.2, tier, source != unavailable) for source in ("a", "b")}

    def partial_after_complete(self, failure="failed"):
        first = self.decide()
        self.assertEqual(first["apply_state"]["policy_basis"], "uninitialized-reference")
        self.assertIsNone(first["apply_state"]["incumbent_before_decision"])
        self.assertEqual(self.engine.previous, Configuration({"a": "high", "b": "high"}))
        self.tiers("medium")
        self.actuator.failure = failure
        partial = self.decide()
        self.assertEqual(partial["apply_state"]["incumbent_before_decision"]["tiers"], {"a": "high", "b": "high"})
        self.assertEqual(self.engine.confirmed_tiers, {"a": "medium", "b": None})
        self.assertIsNone(self.engine.previous)
        self.assertIsNone(self.engine.last_applied)
        self.assertEqual(self.actuator.rates, {"a": 20, "b": 20})
        return partial

    def test_partial_after_complete_retries_only_unknown_and_uses_confirmed_next_cost(self):
        partial = self.partial_after_complete()
        before = Configuration({"a": "high", "b": "high"})
        target = Configuration({"a": "medium", "b": "medium"})
        expected_gain = (configuration_utility(target, self.items, "moderate", self.engine.aqmc_parameters, self.engine.sacm_parameters)
                         - configuration_utility(before, self.items, "moderate", self.engine.aqmc_parameters, self.engine.sacm_parameters))
        self.assertAlmostEqual(partial["sacm"]["utility_gain"], expected_gain)
        self.assertAlmostEqual(partial["sacm"]["transition_cost"], 0.0001)

        # A different current planner candidate cannot become a fictitious incumbent.
        self.tiers("low")
        self.actuator.calls.clear()
        retry = self.decide()
        self.assertEqual(retry["aqmc"]["configuration"]["tiers"], {"a": "low", "b": "low"})
        self.assertEqual(retry["sacm"]["configuration"]["tiers"], target.tiers)
        self.assertEqual(retry["sacm"]["action"], "reconciliation-pending")
        self.assertEqual(self.actuator.calls, [("b", 20)])
        for field in ("incumbent_feasible", "candidate_utility", "incumbent_utility", "utility_gain", "transition_cost", "estimated_update_bytes"):
            self.assertIsNone(retry["sacm"][field])
        self.assertEqual(retry["apply_state"]["confirmed_tiers_before"], {"a": "medium", "b": None})
        self.assertEqual(retry["apply_state"]["confirmation_evidence_before"]["b"]["status"], "unknown-after-attempt")
        self.assertEqual(retry["apply_state"]["status"], "confirmed")

        final = self.decide()
        low = Configuration({"a": "low", "b": "low"})
        expected_utility = configuration_utility(target, self.items, "moderate", self.engine.aqmc_parameters, self.engine.sacm_parameters)
        self.assertEqual(final["apply_state"]["incumbent_before_decision"]["tiers"], target.tiers)
        self.assertAlmostEqual(final["sacm"]["incumbent_utility"], expected_utility)
        self.assertAlmostEqual(final["sacm"]["utility_gain"], configuration_utility(low, self.items, "moderate", self.engine.aqmc_parameters, self.engine.sacm_parameters) - expected_utility)
        self.assertEqual(transition_metrics(target, low, 2), (0, 2, 192))
        self.assertAlmostEqual(final["sacm"]["transition_cost"], 0.0001)
        self.assertEqual(final["sacm"]["estimated_update_bytes"], 192)

    def test_timeout_mismatch_unverified_and_wrong_source_invalidate_only_attempted_node(self):
        for failure in ("timeout", "mismatch", "unverified", "wrong-source"):
            with self.subTest(failure=failure):
                # Establish a fresh full state for every failure variant.
                self.tiers("high")
                self.actuator.failure = None
                self.engine.confirmed_tiers = {"a": None, "b": None}
                self.engine.confirmation_evidence = {source: {"status": "uninitialized"} for source in ("a", "b")}
                self.engine.pending_configuration = None
                self.partial_after_complete(failure)
                self.actuator.calls.clear()
                retry = self.decide()
                self.assertEqual(self.actuator.calls, [("b", 20)])
                self.assertEqual(retry["sacm"]["action"], "reconciliation-pending")

    def test_pending_target_over_budget_blocks_retry_even_if_new_candidate_is_feasible(self):
        self.partial_after_complete()
        self.engine.aqmc_parameters = AqmcParameters(rate_profile="hardware-40hz", airtime_budget_percent=0.2)
        self.tiers("low")
        self.actuator.calls.clear()
        result = self.decide()
        self.assertTrue(result["sacm"]["candidate_feasible"])
        self.assertFalse(result["apply_state"]["pending_target_admissible"])
        self.assertEqual(result["sacm"]["reason"], "pending-target-not-admissible")
        self.assertEqual(self.actuator.calls, [])
        self.assertIsNone(self.engine.confirmed_tiers["b"])

    def test_pending_unavailable_node_blocks_retry_and_preserves_successful_confirmation(self):
        self.partial_after_complete()
        self.tiers("medium", unavailable="b")
        self.actuator.calls.clear()
        result = self.decide()
        self.assertEqual(self.actuator.calls, [])
        self.assertEqual(result["actuation"][0]["status"], "skipped-unavailable")
        self.assertEqual(self.engine.confirmed_tiers, {"a": "medium", "b": None})
        self.assertFalse(result["apply_state"]["pending_target_admissible"])

    def test_inhibit_does_not_invalidate_known_states_or_claim_application(self):
        self.decide()
        before = dict(self.engine.confirmation_evidence)
        self.tiers("medium")
        with tempfile.TemporaryDirectory() as directory:
            inhibit = Path(directory) / "inhibit"
            inhibit.touch()
            self.engine.actuation_inhibit_file = inhibit
            self.actuator.calls.clear()
            result = self.decide()
        self.assertEqual(self.actuator.calls, [])
        self.assertEqual(self.engine.confirmed_tiers, {"a": "high", "b": "high"})
        self.assertEqual(self.engine.confirmation_evidence, before)
        self.assertEqual(result["apply_state"]["status"], "reconciliation-pending")
        self.assertTrue(all(a["status"] == "inhibited" for a in result["actuation"]))

    def test_all_committed_then_unchanged_has_no_redundant_commands(self):
        self.decide()
        self.actuator.calls.clear()
        result = self.decide()
        self.assertEqual(result["sacm"]["action"], "hold")
        self.assertEqual(result["apply_state"]["policy_basis"], "confirmed-state")
        self.assertEqual(self.actuator.calls, [])
        self.assertEqual(result["apply_state"]["status"], "confirmed")

    def test_recommend_and_observe_keep_previous_policy_semantics(self):
        for mode in ("recommend", "observe"):
            with self.subTest(mode=mode):
                self.engine.mode = mode
                self.engine.previous = None
                self.decide()
                result = self.decide()
                self.assertEqual(result["sacm"]["action"], "hold")
                self.assertEqual(result["apply_state"]["policy_basis"], "previous-policy-output")
                self.assertEqual(self.actuator.calls, [])

    def test_summary_accepts_pending_null_metrics_without_counting_hold(self):
        self.partial_after_complete()
        result = self.decide()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "decisions.ndjson"
            path.write_text(json.dumps(result) + "\n")
            report = summarize_decisions(path)
        self.assertEqual(report["action_counts"], {"reconciliation-pending": 1})
        self.assertEqual(report["estimated_accepted_update_bytes"], 0)
        self.assertEqual(report["unknown_update_estimate_decisions"], 1)

    def test_startup_without_attempt_does_not_freeze_initial_target(self):
        self.tiers("high", unavailable="b")
        first = self.decide()
        self.assertFalse(first["apply_armed"])
        self.assertEqual(self.actuator.calls, [])
        self.tiers("medium")
        second = self.decide()
        self.assertEqual(second["apply_state"]["policy_basis"], "uninitialized-reference")
        self.assertEqual(self.actuator.calls, [("a", 20), ("b", 20)])
        self.assertEqual(self.engine.confirmed_tiers, {"a": "medium", "b": "medium"})

    def test_skipped_unavailable_node_keeps_prior_confirmation_and_mixed_vector(self):
        self.decide()
        prior_b = dict(self.engine.confirmation_evidence["b"])
        self.tiers("medium", unavailable="b")
        self.actuator.calls.clear()
        partial = self.decide()
        self.assertEqual(self.actuator.calls, [("a", 20)])
        self.assertEqual(partial["actuation"][1]["status"], "skipped-unavailable")
        self.assertEqual(self.engine.confirmed_tiers, {"a": "medium", "b": "high"})
        self.assertEqual(self.engine.confirmation_evidence["b"], prior_b)
        self.tiers("medium")
        self.actuator.calls.clear()
        next_record = self.decide()
        self.assertEqual(next_record["apply_state"]["incumbent_before_decision"]["tiers"], {"a": "medium", "b": "high"})
        self.assertEqual(self.actuator.calls, [("b", 20)])
        self.assertAlmostEqual(next_record["sacm"]["transition_cost"], 0.00005)

    def test_summary_does_not_mark_unknown_pending_state_as_readmitted(self):
        self.partial_after_complete()
        self.engine.aqmc_parameters = AqmcParameters(rate_profile="hardware-40hz", airtime_budget_percent=0.2)
        self.tiers("low")
        result = self.decide()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "decisions.ndjson"
            path.write_text(json.dumps(result) + "\n")
            with mock.patch("cws_control.reporting.load_operator_events", return_value=([], {"b": {"start": [1], "reconnected": [2]}})):
                report = summarize_decisions(path)
        self.assertIsNone(report["failure_validation"]["b"]["configuration_readmitted_wall_time_ns"])

    def test_pending_target_checks_availability_of_already_confirmed_active_node(self):
        self.partial_after_complete()
        self.tiers("medium", unavailable="a")
        self.actuator.calls.clear()
        result = self.decide()
        self.assertTrue(self.items["b"].available)
        self.assertEqual(self.actuator.calls, [])
        self.assertFalse(result["apply_state"]["pending_target_admissible"])
        self.assertEqual(result["actuation"][0]["status"], "reconciliation-blocked")
        self.assertEqual(self.engine.confirmed_tiers, {"a": "medium", "b": None})
