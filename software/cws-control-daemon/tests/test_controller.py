from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cws_control.engine import ControlEngine
from cws_control.context import classify_load
from cws_control.features import amplitude_features, histogram_entropy, iq_to_amplitude
from cws_control.models import Configuration, Envelope, NodeEvidence
from cws_control.parser import ParseError, parse_csi, parse_envelope, parse_record
from cws_control.policy import (
    AqmcParameters,
    SacmParameters,
    aqmc_admit,
    aqmc_direct_decide,
    aqmc_utility,
    budget,
    sacm_decide,
    tier_for_utility,
)
from cws_control.timing import WRAP_US, TimingValidator
from cws_control.scenarios import run_failure_scenarios
from cws_control.reporting import summarize_decisions
from cws_control.sweep import run_rate_sweep
from cws_control.prpl import extract_prpl_context
from cws_control.method_blocks import run_method_blocks


def raw_csi(sequence: int, timestamp_us: int, rssi: int = -45, values: list[int] | None = None) -> str:
    values = values or [3, 4, 5, 12]
    return (
        f'CSI_DATA,{sequence},aa:bb:cc:dd:ee:ff,{rssi},1,1,0,0,0,0,0,0,0,0,-95,0,6,0,'
        f'{timestamp_us},0,128,0,{len(values)},0,"{values}"'
    )


def envelope(source: str, sequence: int, wall_ns: int, mono_ns: int, raw: str) -> dict:
    return {
        "source_id": source,
        "session_id": "session-test",
        "connection_epoch": 1,
        "source_sequence": sequence,
        "ingest_wall_time_ns": wall_ns,
        "ingest_monotonic_ns": mono_ns,
        "raw": raw,
    }


def evidence(source: str, utility: float, tier: str, available: bool = True) -> NodeEvidence:
    return NodeEvidence(
        source_id=source,
        available=available,
        unavailable_reasons=() if available else ("stale-csi",),
        frame_count=20,
        expected_frames=20.0,
        rssi_mean=-45.0,
        rssi_std=2.0,
        frequency_spread_hz=1.0,
        csi_mean_amplitude=10.0,
        csi_variance=4.0,
        csi_rms=10.2,
        csi_entropy=2.0,
        confidence=0.9,
        reliability=0.9 if available else 0.0,
        timing_valid=available,
        timing_uncertainty_ms=1.0,
        timing_drift_ppm=0.0,
        timing_points=20,
        trigger=False,
        utility=utility,
        requested_tier=tier,
        motion_score=0.2,
        boot_epoch=1,
        output_drop_ratio=0.0,
        chip_temperature_c=55.0,
    )


class ParserTests(unittest.TestCase):
    def test_collector_envelope_and_csi(self) -> None:
        value = envelope("esp32-s3-a", 1, 10, 20, raw_csi(7, 1234))
        parsed_envelope = parse_envelope(json.dumps(value))
        frame = parse_record(parsed_envelope)
        self.assertEqual(frame.sequence, 7)
        self.assertEqual(frame.local_timestamp_us, 1234)
        self.assertEqual(frame.csi, (3, 4, 5, 12))

    def test_rejects_declared_length_mismatch(self) -> None:
        value = Envelope("a", "s", 1, 1, 1, 1, raw_csi(1, 1).replace(',4,0,"', ',6,0,"'))
        with self.assertRaises(ParseError):
            parse_csi(value)

    def test_heartbeat(self) -> None:
        value = envelope(
            "esp32-s3-a",
            1,
            10,
            20,
            "CWSLAB_TIMING_HEARTBEAT boot_epoch=3 connected=1 ping_hz=20",
        )
        heartbeat = parse_record(parse_envelope(value))
        self.assertEqual(heartbeat.integer("boot_epoch"), 3)
        self.assertEqual(heartbeat.integer("ping_hz"), 20)


class FeatureTests(unittest.TestCase):
    def test_iq_and_entropy(self) -> None:
        self.assertEqual(iq_to_amplitude([3, 4, 5, 12]), [5.0, 13.0])
        self.assertEqual(histogram_entropy([1.0, 1.0, 1.0]), 0.0)

    def test_published_amplitude_feature_path(self) -> None:
        frame = parse_csi(Envelope("a", "s", 1, 1, 1, 1, raw_csi(1, 1)))
        mean, variance, rms, entropy = amplitude_features([frame])
        self.assertEqual(mean, 9.0)
        self.assertEqual(variance, 16.0)
        self.assertAlmostEqual(rms, math.sqrt(97.0))
        self.assertEqual(entropy, 1.0)


class ContextTests(unittest.TestCase):
    def test_declared_load_thresholds(self) -> None:
        self.assertEqual(classify_load(10.0, 5.0, 0.0), "light")
        self.assertEqual(classify_load(30.0, 30.0, 0.0), "moderate")
        self.assertEqual(classify_load(60.0, 70.0, 3.0), "elevated")
        self.assertEqual(classify_load(80.0, 120.0, 15.0), "high")

    def test_prpl_tr181_extraction(self) -> None:
        value = extract_prpl_context(
            {
                "node_id": "prpl-router",
                "agent_version": "1",
                "sampled_epoch_ms": "1234",
                "radio": '{"Channel":6,"ChannelUtilization":191,"Noise":-94,"ErrorsSent":2}',
                "stations": "AssociatedDeviceNumberOfEntries=3",
                "qos": '{"DiscardPackets":4}',
                "mesh": "",
            },
            received_wall_time_ns=5_000,
        )
        self.assertEqual(value["node_id"], "prpl-router")
        self.assertEqual(value["load"], "elevated")
        self.assertAlmostEqual(value["radio_channel_utilization_percent_max"], 19100 / 255)
        self.assertEqual(value["associated_device_count"], 3)
        self.assertEqual(value["qos_drop_counter_values"], [4.0])
        self.assertTrue(value["telemetry_valid"])

    def test_pwhm_channel_load_is_used_directly_as_percent(self) -> None:
        value = extract_prpl_context(
            {
                "node_id": "prpl-router-pwhm",
                "agent_version": "1.1.0",
                "sampled_epoch_ms": "1234",
                "radio": '{"Channel":36,"ChannelLoad":7,"Noise":0}',
                "stations": "AssociatedDeviceNumberOfEntries=0",
            },
            received_wall_time_ns=5_000,
        )
        self.assertEqual(value["radio_channel_utilization_percent_max"], 7.0)
        self.assertEqual(value["load"], "light")
        self.assertTrue(value["telemetry_valid"])


class TimingTests(unittest.TestCase):
    def test_wrap_is_not_a_reboot(self) -> None:
        validator = TimingValidator()
        raws = [WRAP_US - 3000, WRAP_US - 2000, WRAP_US - 1000, 0, 1000, 2000, 3000, 4000]
        estimates = [validator.observe(raw, index * 1_000_000, 7) for index, raw in enumerate(raws)]
        self.assertEqual(estimates[3].event, "timestamp-wrap")
        self.assertEqual(estimates[-1].boot_epoch, 7)
        self.assertTrue(estimates[-1].valid)

    def test_non_wrap_regression_starts_new_epoch(self) -> None:
        validator = TimingValidator()
        validator.observe(10_000, 1_000_000, 4)
        result = validator.observe(500, 2_000_000, 4)
        self.assertEqual(result.event, "inferred-clock-reset")
        self.assertEqual(result.boot_epoch, 5)

    def test_naive_baseline_misclassifies_wrap_as_reset(self) -> None:
        validator = TimingValidator(mode="naive-reset-on-regression")
        validator.observe(WRAP_US - 100, 1_000_000, 7)
        result = validator.observe(100, 2_000_000, 7)
        self.assertEqual(result.event, "raw-regression-reset")

    def test_rollover_baseline_ignores_reported_boot_epoch(self) -> None:
        validator = TimingValidator(mode="rollover-aware-affine")
        first = validator.observe(1_000, 1_000_000, 7)
        second = validator.observe(2_000, 2_000_000, 8)
        self.assertIsNone(first.event)
        self.assertIsNone(second.event)


class AqmcTests(unittest.TestCase):
    def test_published_utility_and_tiers(self) -> None:
        self.assertAlmostEqual(aqmc_utility(1.0, 5.0, 1.0), 1.0)
        self.assertEqual(tier_for_utility(0.74, False), "low")
        self.assertEqual(tier_for_utility(0.75, False), "medium")
        self.assertEqual(tier_for_utility(1.2, False), "high")

    def test_paper_airtime_equation(self) -> None:
        parameters = AqmcParameters(rate_profile="paper")
        result = budget(Configuration({"a": "high", "b": "high"}), "moderate", parameters)
        expected = 100 * 512 * 8 * 20 / (54 * 1e6)
        self.assertAlmostEqual(result.airtime_percent, expected)
        self.assertAlmostEqual(result.qos_penalty_percent, 0.35 * expected)

    def test_greedy_admission_skips_unavailable(self) -> None:
        result = aqmc_admit(
            {"a": evidence("a", 1.3, "high"), "b": evidence("b", 2.0, "high", False)},
            "moderate",
            AqmcParameters(),
        )
        self.assertEqual(result.configuration.active_nodes, ("a",))
        self.assertEqual(result.rejected["b"], "unavailable")


class SacmTests(unittest.TestCase):
    def setUp(self) -> None:
        self.aqmc = AqmcParameters()
        self.sacm = SacmParameters()
        self.items = {"a": evidence("a", 1.0, "medium"), "b": evidence("b", 0.9, "medium")}

    def test_holds_when_gain_does_not_cover_transition(self) -> None:
        previous = Configuration({"a": "medium", "b": "off"})
        candidate = Configuration({"a": "medium", "b": "medium"})
        result = sacm_decide(previous, candidate, self.items, "moderate", self.aqmc, self.sacm)
        self.assertEqual(result.action, "hold")
        self.assertEqual(result.configuration, previous)

    def test_unchanged_candidate_is_an_operational_hold(self) -> None:
        previous = Configuration({"a": "medium", "b": "off"})
        result = sacm_decide(previous, previous, self.items, "moderate", self.aqmc, self.sacm)
        self.assertEqual(result.action, "hold")
        self.assertEqual(result.reason, "unchanged")
        self.assertEqual(result.estimated_update_bytes, 0)

    def test_infeasible_incumbent_cannot_be_held(self) -> None:
        items = {**self.items, "a": evidence("a", 1.0, "medium", False)}
        previous = Configuration({"a": "medium", "b": "off"})
        candidate = Configuration({"a": "off", "b": "medium"})
        result = sacm_decide(previous, candidate, items, "moderate", self.aqmc, self.sacm)
        self.assertEqual(result.action, "accept")
        self.assertEqual(result.reason, "feasibility-override")

    def test_fallback_when_no_node_is_available(self) -> None:
        items = {"a": evidence("a", 1.0, "medium", False)}
        result = sacm_decide(
            Configuration({"a": "medium"}),
            Configuration({"a": "off"}),
            items,
            "moderate",
            self.aqmc,
            self.sacm,
        )
        self.assertEqual(result.action, "fallback")
        self.assertEqual(result.configuration.tier("a"), "off")

    def test_aqmc_only_ablation_uses_candidate_without_maintenance_cost(self) -> None:
        previous = Configuration({"a": "high", "b": "off"})
        candidate = Configuration({"a": "low", "b": "medium"})
        result = aqmc_direct_decide(
            previous, candidate, self.items, "moderate", self.aqmc, self.sacm
        )
        self.assertEqual(result.configuration, candidate)
        self.assertEqual(result.reason, "aqmc-direct")
        self.assertEqual(result.transition_cost, 0.0)


class EngineTests(unittest.TestCase):
    def config(self) -> dict:
        return {
            "schema_version": 1,
            "nodes": {"esp32-s3-a": {}, "esp32-s3-b": {}},
            "window_seconds": 2.0,
            "freshness_seconds": 5.0,
            "minimum_frames": 4,
            "mode": "recommend",
            "listen_host": "127.0.0.1",
            "evidence_transport": "loopback-authenticated-tunnel/1",
            "load": "moderate",
            "aqmc": {"rate_profile": "hardware-40hz"},
        }

    def add_live_node(self, engine: ControlEngine, source: str, origin: int) -> None:
        heartbeat = "CWSLAB_TIMING_HEARTBEAT boot_epoch=1 connected=1 ping_hz=10 csi_count=0 output_drops=0 chip_temp_millicelsius=55000"
        engine.ingest(envelope(source, 1, origin, origin, heartbeat))
        for index in range(12):
            offset = (index + 1) * 100_000_000
            engine.ingest(
                envelope(
                    source,
                    index + 2,
                    origin + offset,
                    origin + offset,
                    raw_csi(index, (index + 1) * 100_000, -45 + (index % 3)),
                )
            )

    def test_two_node_decision_is_auditable(self) -> None:
        origin = 1_000_000_000_000
        engine = ControlEngine(self.config())
        self.add_live_node(engine, "esp32-s3-a", origin)
        self.add_live_node(engine, "esp32-s3-b", origin)
        decision = engine.decide(origin + 1_300_000_000)
        self.assertEqual(decision["method"]["coordination"], "AQ-MC")
        self.assertEqual(decision["method"]["maintenance"], "SACM")
        self.assertEqual(decision["fusion"]["quorum"], 2)
        self.assertEqual(decision["sacm"]["action"], "accept")
        self.assertEqual(sorted(decision["sacm"]["configuration"]["tiers"]), ["esp32-s3-a", "esp32-s3-b"])

    def test_missing_node_is_excluded_not_fused(self) -> None:
        origin = 1_000_000_000_000
        engine = ControlEngine(self.config())
        self.add_live_node(engine, "esp32-s3-a", origin)
        decision = engine.decide(origin + 1_300_000_000)
        self.assertEqual(decision["fusion"]["quorum"], 1)
        self.assertFalse(decision["evidence"]["esp32-s3-b"]["available"])
        self.assertEqual(decision["sacm"]["configuration"]["tiers"]["esp32-s3-b"], "off")

    def test_actuation_inhibit_is_audited_and_blocks_commands(self) -> None:
        class RecordingActuator:
            def __init__(self) -> None:
                self.calls: list[tuple[str, int]] = []

            def apply(self, source_id: str, hz: int) -> dict:
                self.calls.append((source_id, hz))
                return {
                    "schema_version": "cws-actuation-transaction/1",
                    "source_id": source_id,
                    "hz": hz,
                    "status": "applied",
                    "transaction_status": "committed",
                    "verified_committed": True,
                }

        origin = 1_000_000_000_000
        with tempfile.TemporaryDirectory() as temporary:
            inhibit = Path(temporary) / "inhibit"
            inhibit.touch()
            config = self.config()
            config.update({
                "mode": "apply",
                "apply_startup_quorum": 1,
                "actuation_inhibit_file": str(inhibit),
                "actuators": {"esp32-s3-a": {}, "esp32-s3-b": {}},
            })
            actuator = RecordingActuator()
            engine = ControlEngine(config, actuator=actuator)
            self.add_live_node(engine, "esp32-s3-a", origin)
            decision = engine.decide(origin + 1_300_000_000)
        self.assertTrue(decision["actuation_inhibited"])
        self.assertTrue(decision["actuation"])
        self.assertTrue(all(action["status"] == "inhibited" for action in decision["actuation"]))
        self.assertEqual(actuator.calls, [])

    def test_apply_waits_for_startup_quorum_and_skips_failed_node(self) -> None:
        class RecordingActuator:
            def __init__(self) -> None:
                self.calls: list[tuple[str, int]] = []

            def apply(self, source_id: str, hz: int) -> dict:
                self.calls.append((source_id, hz))
                return {
                    "schema_version": "cws-actuation-transaction/1",
                    "source_id": source_id,
                    "hz": hz,
                    "status": "applied",
                    "transaction_status": "committed",
                    "verified_committed": True,
                }

        origin = 1_000_000_000_000
        config = self.config()
        config.update({"mode": "apply", "actuators": {"esp32-s3-a": {}, "esp32-s3-b": {}}})
        actuator = RecordingActuator()
        engine = ControlEngine(config, actuator=actuator)

        self.add_live_node(engine, "esp32-s3-a", origin)
        startup = engine.decide(origin + 1_300_000_000)
        self.assertFalse(startup["apply_armed"])
        self.assertEqual(startup["actuation"], [])
        self.assertEqual(actuator.calls, [])

        self.add_live_node(engine, "esp32-s3-b", origin)
        armed = engine.decide(origin + 1_400_000_000)
        self.assertTrue(armed["apply_armed"])
        self.assertTrue(actuator.calls)

        actuator.calls.clear()
        failed = engine.decide(origin + 6_500_000_000)
        self.assertEqual(failed["fusion"]["quorum"], 0)
        self.assertEqual(actuator.calls, [])
        self.assertTrue(all(action["status"] == "skipped-unavailable" for action in failed["actuation"]))

    def test_apply_retries_after_partial_actuator_failure(self) -> None:
        class FlakyActuator:
            def __init__(self) -> None:
                self.calls: list[tuple[str, int]] = []
                self.failed_once = False

            def apply(self, source_id: str, hz: int) -> dict:
                self.calls.append((source_id, hz))
                if source_id == "esp32-s3-b" and not self.failed_once:
                    self.failed_once = True
                    return {"source_id": source_id, "hz": hz, "status": "failed"}
                return {
                    "schema_version": "cws-actuation-transaction/1",
                    "source_id": source_id,
                    "hz": hz,
                    "status": "applied",
                    "transaction_status": "committed",
                    "verified_committed": True,
                }

        origin = 1_000_000_000_000
        config = self.config()
        config.update({"mode": "apply", "actuators": {"esp32-s3-a": {}, "esp32-s3-b": {}}})
        actuator = FlakyActuator()
        engine = ControlEngine(config, actuator=actuator)
        self.add_live_node(engine, "esp32-s3-a", origin)
        self.add_live_node(engine, "esp32-s3-b", origin)

        first = engine.decide(origin + 1_300_000_000)
        self.assertIn("failed", {action["status"] for action in first["actuation"]})
        self.assertIsNone(engine.last_applied)

        actuator.calls.clear()
        retried = engine.decide(origin + 1_400_000_000)
        self.assertTrue(actuator.calls)
        self.assertTrue(all(action["status"] == "applied" for action in retried["actuation"]))
        self.assertIsNotNone(engine.last_applied)

    def test_plain_applied_status_does_not_confirm_configuration(self) -> None:
        class UnverifiedActuator:
            def apply(self, source_id: str, hz: int) -> dict:
                return {"source_id": source_id, "hz": hz, "status": "applied"}

        origin = 1_000_000_000_000
        config = self.config()
        config.update({"mode": "apply", "actuators": {"esp32-s3-a": {}, "esp32-s3-b": {}}})
        engine = ControlEngine(config, actuator=UnverifiedActuator())
        self.add_live_node(engine, "esp32-s3-a", origin)
        self.add_live_node(engine, "esp32-s3-b", origin)
        decision = engine.decide(origin + 1_300_000_000)
        self.assertTrue(decision["actuation"])
        self.assertIsNone(engine.last_applied)

    def test_apply_requires_protected_non_wildcard_evidence_transport(self) -> None:
        unprotected = self.config()
        unprotected.update(
            {
                "mode": "apply",
                "listen_host": "127.0.0.1",
                "evidence_transport": "unauthenticated-udp/1",
            }
        )
        with self.assertRaisesRegex(ValueError, "protected evidence transport"):
            ControlEngine(unprotected)

        wildcard = self.config()
        wildcard.update(
            {
                "mode": "apply",
                "listen_host": "0.0.0.0",
                "evidence_transport": "isolated-experiment-interface/1",
            }
        )
        with self.assertRaisesRegex(ValueError, "wildcard evidence listener"):
            ControlEngine(wildcard)

        exposed_tunnel = self.config()
        exposed_tunnel.update(
            {
                "mode": "apply",
                "listen_host": "192.0.2.10",
                "evidence_transport": "loopback-authenticated-tunnel/1",
            }
        )
        with self.assertRaisesRegex(ValueError, "must bind to loopback"):
            ControlEngine(exposed_tunnel)

    def test_fresh_prpl_context_raises_constraint_load(self) -> None:
        origin = 1_000_000_000_000
        with tempfile.TemporaryDirectory() as temporary:
            context_path = Path(temporary) / "prpl.json"
            context_path.write_text(
                json.dumps({"sampled_wall_time_ns": origin, "load": "high", "source": "prpl"}),
                encoding="utf-8",
            )
            config = self.config()
            config["prpl_context_file"] = str(context_path)
            engine = ControlEngine(config)
            self.add_live_node(engine, "esp32-s3-a", origin)
            decision = engine.decide(origin + 1_000_000_000)
        self.assertEqual(decision["load"], "high")
        self.assertEqual(decision["network_context"]["prpl_status"], "fresh")

    def test_aqmc_only_method_is_explicit(self) -> None:
        origin = 1_000_000_000_000
        config = self.config()
        config["policy_variant"] = "aqmc-only"
        engine = ControlEngine(config)
        self.add_live_node(engine, "esp32-s3-a", origin)
        decision = engine.decide(origin + 1_300_000_000)
        self.assertEqual(decision["method"]["maintenance"], "none")
        self.assertEqual(decision["sacm"]["reason"], "aqmc-direct")


class FailureScenarioTests(unittest.TestCase):
    def test_all_deterministic_failure_scenarios_pass(self) -> None:
        report = run_failure_scenarios()
        self.assertTrue(report["passed"])
        self.assertEqual(report["passed_cases"], report["total_cases"])


class ReportingTests(unittest.TestCase):
    def test_summary_counts_holds_and_budget_safety(self) -> None:
        record = {
            "decision_wall_time_ns": 1_000_000_000,
            "load": "moderate",
            "fusion": {"quorum": 1, "motion_score": 0.2},
            "aqmc": {"budget": {"airtime_percent": 0.1, "qos_penalty_percent": 0.035, "feasible": True}},
            "sacm": {
                "action": "hold",
                "reason": "unchanged",
                "candidate_feasible": True,
                "utility_gain": 0.0,
                "estimated_update_bytes": 0,
                "configuration": {"tiers": {"a": "low"}},
            },
            "timing_events": [],
            "actuation": [],
            "evidence": {
                "a": {
                    "available": True,
                    "unavailable_reasons": [],
                    "confidence": 0.9,
                    "timing_uncertainty_ms": 1.0,
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "decisions.ndjson"
            path.write_text(json.dumps(record) + "\n", encoding="utf-8")
            summary = summarize_decisions(path)
        self.assertEqual(summary["decisions"], 1)
        self.assertEqual(summary["action_counts"], {"hold": 1})
        self.assertEqual(summary["accepted_budget_violation_decisions"], 0)

    def test_failure_markers_produce_detection_and_recovery_latencies(self) -> None:
        def decision(wall_ns: int, available: bool, timing_valid: bool, quorum: int, tier: str) -> dict:
            return {
                "decision_wall_time_ns": wall_ns,
                "load": "moderate",
                "fusion": {"quorum": quorum, "motion_score": 0.2},
                "aqmc": {"budget": {"airtime_percent": 0.1, "qos_penalty_percent": 0.035, "feasible": True}},
                "sacm": {
                    "action": "hold", "reason": "unchanged", "candidate_feasible": True,
                    "utility_gain": 0.0, "estimated_update_bytes": 0,
                    "configuration": {"tiers": {"esp32-s3-b": tier}},
                },
                "timing_events": [], "actuation": [],
                "evidence": {
                    "esp32-s3-b": {
                        "available": available,
                        "unavailable_reasons": [] if available else ["stale-csi"],
                        "confidence": 0.9 if available else 0.0,
                        "timing_uncertainty_ms": 1.0 if timing_valid else None,
                        "timing_valid": timing_valid,
                    }
                },
            }

        samples = [
            decision(1_000_000_000, True, True, 1, "high"),
            decision(3_000_000_000, False, False, 0, "off"),
            decision(5_000_000_000, True, True, 1, "medium"),
        ]
        events = [
            {"event_type": "failure-esp32-s3-b-start", "wall_time_ns": 2_000_000_000},
            {"event_type": "failure-esp32-s3-b-reconnected", "wall_time_ns": 4_000_000_000},
        ]
        with tempfile.TemporaryDirectory() as temporary:
            decisions_path = Path(temporary) / "decisions.ndjson"
            events_path = Path(temporary) / "events.ndjson"
            decisions_path.write_text("".join(json.dumps(item) + "\n" for item in samples), encoding="utf-8")
            events_path.write_text("".join(json.dumps(item) + "\n" for item in events), encoding="utf-8")
            summary = summarize_decisions(decisions_path, [events_path])
        failure = summary["failure_validation"]["esp32-s3-b"]
        self.assertEqual(failure["detection_latency_seconds"], 1.0)
        self.assertEqual(failure["timing_recovered_latency_seconds"], 1.0)
        self.assertEqual(failure["quorum_recovered_latency_seconds"], 1.0)
        self.assertEqual(failure["configuration_readmitted_latency_seconds"], 1.0)
        self.assertEqual(summary["availability_outages"]["esp32-s3-b"][0]["duration_seconds"], 2.0)


class SweepTests(unittest.TestCase):
    def test_acknowledged_phase_sequence(self) -> None:
        class VerifiedActuator:
            def __init__(self, definitions: dict, timeout_seconds: float) -> None:
                self.definitions = definitions

            def apply(self, source_id: str, hz: int) -> dict:
                return {
                    "schema_version": "cws-actuation-transaction/1",
                    "source_id": source_id,
                    "hz": hz,
                    "transaction_id": f"txn-{source_id}-{hz}",
                    "transaction_status": "committed",
                    "status": "applied",
                    "verified_committed": True,
                }

        config = {
            "actuators": {
                "a": {
                    "argv": ["/usr/bin/true", "{hz}"],
                    "event_argv": ["/usr/bin/true", "{value}"],
                },
                "b": {
                    "argv": ["/usr/bin/true", "{hz}"],
                    "event_argv": ["/usr/bin/true", "{value}"],
                },
            }
        }
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "sweep.json"
            with mock.patch("cws_control.sweep.CommandActuator", VerifiedActuator):
                report = run_rate_sweep(config, [10, 20, 40], 0.001, output)
            persisted = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "complete")
        self.assertEqual([phase["rate_hz"] for phase in report["phases"]], [10, 20, 40])
        self.assertEqual(persisted["status"], "complete")


class MethodBlockTests(unittest.TestCase):
    def test_seeded_static_aqmc_and_full_blocks_complete(self) -> None:
        class VerifiedActuator:
            def __init__(self, definitions: dict, timeout_seconds: float) -> None:
                self.definitions = definitions

            def apply(self, source_id: str, hz: int) -> dict:
                return {
                    "schema_version": "cws-actuation-transaction/1",
                    "source_id": source_id,
                    "hz": hz,
                    "transaction_id": f"txn-{source_id}-{hz}",
                    "transaction_status": "committed",
                    "status": "applied",
                    "verified_committed": True,
                }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            controller = root / "controller.json"
            controller.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "listen_host": "127.0.0.1",
                        "listen_port": 0,
                        "decision_interval_seconds": 0.001,
                        "window_seconds": 1,
                        "freshness_seconds": 1,
                        "minimum_frames": 1,
                        "mode": "apply",
                        "evidence_transport": "loopback-authenticated-tunnel/1",
                        "nodes": {"a": {}},
                        "aqmc": {"rate_profile": "hardware-40hz"},
                        "actuators": {
                            "a": {
                                "argv": ["/usr/bin/true", "{hz}"],
                                "event_argv": ["/usr/bin/true", "{value}"],
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch("cws_control.method_blocks.CommandActuator", VerifiedActuator):
                report = run_method_blocks(
                    {
                        "controller_config": str(controller),
                        "seed": 10,
                        "repetitions": 1,
                        "phase_seconds": 0.01,
                        "cooldown_seconds": 0,
                        "require_prpl_context": False,
                    },
                    root / "output",
                )
        self.assertEqual(report["status"], "complete")
        self.assertEqual({phase["method"] for phase in report["phases"]}, {
            "static-40", "aqmc-only", "aqmc-sacm"
        })


if __name__ == "__main__":
    unittest.main()
