from __future__ import annotations

import json
import ipaddress
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from cws_control.contracts import (
    ACTUATION_TRANSACTION_V1,
    ContractError,
    actuation_transaction,
    require_hash,
    require_wire_id,
)
from cws_control.features import amplitude_features, frequency_spread_hz, rssi_features
from cws_control.models import CsiFrame, Configuration, Heartbeat, NodeEvidence, clamp, json_ready
from cws_control.ordering import OrderingState
from cws_control.parser import ParseError, parse_envelope, parse_record
from cws_control.policy import (
    AqmcParameters,
    SacmParameters,
    aqmc_admit,
    aqmc_utility,
    aqmc_direct_decide,
    sacm_decide,
    tier_for_utility,
)
from cws_control.timing import TimingValidator


@dataclass
class NodeState:
    source_id: str
    frames: deque[CsiFrame] = field(default_factory=deque)
    timing: TimingValidator = field(default_factory=TimingValidator)
    heartbeat: Heartbeat | None = None
    timing_estimate: Any = None
    parse_errors: int = 0
    last_drop_count: int | None = None
    last_csi_count: int | None = None
    output_drop_ratio: float = 0.0
    pending_timing_events: list[dict[str, Any]] = field(default_factory=list)

    def update_heartbeat(self, heartbeat: Heartbeat) -> None:
        drop_count = heartbeat.integer("output_drops")
        csi_count = heartbeat.integer("csi_count")
        if (
            drop_count is not None
            and csi_count is not None
            and self.last_drop_count is not None
            and self.last_csi_count is not None
        ):
            drops = max(0, drop_count - self.last_drop_count)
            produced = max(0, csi_count - self.last_csi_count)
            self.output_drop_ratio = drops / max(1, drops + produced)
        self.last_drop_count = drop_count
        self.last_csi_count = csi_count
        self.heartbeat = heartbeat


class CommandActuator:
    """Apply accepted rates with argv templates; no shell is involved."""

    def __init__(self, definitions: Mapping[str, Mapping[str, Any]], timeout_seconds: float = 10.0):
        self.definitions = definitions
        self.timeout_seconds = timeout_seconds

    def apply(
        self,
        source_id: str,
        hz: int,
        decision_sha256: str | None = None,
        transaction_id: str | None = None,
    ) -> dict[str, Any]:
        definition = self.definitions.get(source_id)
        if not definition:
            return {"source_id": source_id, "hz": hz, "status": "no-actuator"}
        template = definition.get("argv")
        if not isinstance(template, list) or not template or not all(isinstance(item, str) for item in template):
            return {"source_id": source_id, "hz": hz, "status": "invalid-actuator"}
        if decision_sha256 is None and transaction_id is not None:
            return {
                "source_id": source_id,
                "hz": hz,
                "status": "invalid-actuation-context",
                "reason": "transaction-without-decision",
            }
        if decision_sha256 is not None:
            try:
                require_hash(decision_sha256, "decision-sha256")
                require_wire_id(transaction_id, "transaction-id")
            except ContractError as exc:
                return {
                    "source_id": source_id,
                    "hz": hz,
                    "status": "invalid-actuation-context",
                    "reason": str(exc),
                }
        argv = []
        for item in template:
            rendered = item.replace("{source_id}", source_id).replace("{hz}", str(hz))
            if decision_sha256 is not None:
                rendered = rendered.replace("{decision_sha256}", decision_sha256)
                rendered = rendered.replace("{transaction_id}", str(transaction_id))
            argv.append(rendered)
        try:
            completed = subprocess.run(argv, capture_output=True, text=True, timeout=self.timeout_seconds, check=False)
            if completed.returncode != 0:
                return {
                    "source_id": source_id,
                    "hz": hz,
                    "status": "failed",
                    "reason": "nonzero-returncode",
                    "returncode": completed.returncode,
                    "stderr": completed.stderr[-2000:],
                }
            try:
                result = json.loads(completed.stdout)
            except json.JSONDecodeError:
                return {
                    "source_id": source_id,
                    "hz": hz,
                    "status": "failed",
                    "reason": "invalid-json-result",
                    "returncode": completed.returncode,
                }
            try:
                result = actuation_transaction(
                    result,
                    expected_source_id=source_id,
                    expected_hz=hz,
                    expected_decision_sha256=decision_sha256,
                )
                if transaction_id is not None and result["transaction_id"] != transaction_id:
                    raise ContractError("transaction-id-mismatch")
                if decision_sha256 is None and result["schema_version"] != ACTUATION_TRANSACTION_V1:
                    raise ContractError("decision-context-required")
                if result["transaction_status"] != "committed":
                    raise ContractError("terminal-status-mismatch")
            except ContractError as exc:
                return {
                    "source_id": source_id,
                    "hz": hz,
                    "status": "mismatch",
                    "reason": str(exc),
                    "returncode": completed.returncode,
                }
            return {
                "schema_version": result["schema_version"],
                "source_id": source_id,
                "hz": hz,
                "transaction_id": result["transaction_id"],
                "decision_sha256": result.get("decision_sha256"),
                "transaction_status": "committed",
                "status": "applied",
                "verified_committed": True,
                "terminal_facts_sha256": result["terminal_facts_sha256"],
                "returncode": completed.returncode,
            }
        except subprocess.TimeoutExpired as exc:
            return {
                "source_id": source_id,
                "hz": hz,
                "status": "timeout",
                "reason": "actuator-timeout",
                "error": str(exc),
            }
        except OSError as exc:
            return {"source_id": source_id, "hz": hz, "status": "error", "error": str(exc)}


class ControlEngine:
    def __init__(self, config: Mapping[str, Any], actuator: CommandActuator | None = None):
        self.config = dict(config)
        self.node_ids = tuple(sorted(self.config.get("nodes", {})))
        if not self.node_ids:
            raise ValueError("controller config must define nodes")
        self.window_seconds = float(self.config.get("window_seconds", 2.0))
        self.freshness_seconds = float(self.config.get("freshness_seconds", 5.0))
        self.default_rate_hz = int(self.config.get("default_rate_hz", 40))
        self.rssi_std_scale_dbm = float(self.config.get("rssi_std_scale_dbm", 5.0))
        self.motion_rssi_std_dbm = float(self.config.get("motion_rssi_std_dbm", 3.0))
        self.motion_csi_cv = float(self.config.get("motion_csi_cv", 0.25))
        self.minimum_frames = int(self.config.get("minimum_frames", 4))
        self.load = str(self.config.get("load", "moderate"))
        self.context_file = Path(self.config["context_file"]) if self.config.get("context_file") else None
        self.context_max_age_seconds = float(self.config.get("context_max_age_seconds", 30.0))
        self.actuation_inhibit_file = (
            Path(self.config["actuation_inhibit_file"])
            if self.config.get("actuation_inhibit_file")
            else None
        )
        self.timing_mode = str(self.config.get("timing_mode", "failure-aware-affine-v1"))
        self.reorder_window_records = int(self.config.get("reorder_window_records", 32))
        self.ordering_event_limit = int(self.config.get("ordering_event_limit", 32))
        self.ordering_context_history = int(self.config.get("ordering_context_history", 16))
        self.context: dict[str, Any] | None = None
        self.prpl_context_file = (
            Path(self.config["prpl_context_file"])
            if self.config.get("prpl_context_file")
            else None
        )
        self.prpl_context_max_age_seconds = float(
            self.config.get("prpl_context_max_age_seconds", 15.0)
        )
        self.prpl_context: dict[str, Any] | None = None
        aqmc_values = self.config.get("aqmc", {})
        self.aqmc_parameters = AqmcParameters(**aqmc_values)
        sacm_values = self.config.get("sacm", {})
        self.sacm_parameters = SacmParameters(**sacm_values)
        self.mode = str(self.config.get("mode", "observe"))
        if self.mode not in {"observe", "recommend", "apply"}:
            raise ValueError("controller mode must be observe, recommend, or apply")
        self.listen_host = str(self.config.get("listen_host", "127.0.0.1"))
        self.evidence_transport = str(
            self.config.get("evidence_transport", "unauthenticated-udp/1")
        )
        allowed_evidence_transports = {
            "unauthenticated-udp/1",
            "loopback-authenticated-tunnel/1",
            "isolated-experiment-interface/1",
        }
        if self.evidence_transport not in allowed_evidence_transports:
            raise ValueError("unknown evidence_transport version")
        if self.mode == "apply":
            if self.evidence_transport == "unauthenticated-udp/1":
                raise ValueError("apply mode requires a protected evidence transport")
            try:
                listen_address = ipaddress.ip_address(self.listen_host)
            except ValueError as exc:
                raise ValueError("apply mode requires an explicit listen IP address") from exc
            if listen_address.is_unspecified:
                raise ValueError("apply mode cannot use a wildcard evidence listener")
            if (
                self.evidence_transport == "loopback-authenticated-tunnel/1"
                and not listen_address.is_loopback
            ):
                raise ValueError("authenticated-tunnel evidence must bind to loopback")
        self.policy_variant = str(self.config.get("policy_variant", "aqmc-sacm"))
        if self.policy_variant not in {"aqmc-only", "aqmc-sacm"}:
            raise ValueError("policy_variant must be aqmc-only or aqmc-sacm")
        self.states = {
            source_id: NodeState(source_id, timing=TimingValidator(mode=self.timing_mode))
            for source_id in self.node_ids
        }
        self.ordering = {
            source_id: OrderingState(
                window=self.reorder_window_records,
                event_limit=self.ordering_event_limit,
                context_history_limit=self.ordering_context_history,
            )
            for source_id in self.node_ids
        }
        self.previous: Configuration | None = None
        self.last_applied: Configuration | None = None
        self.apply_startup_quorum = int(
            self.config.get("apply_startup_quorum", len(self.node_ids))
        )
        if not 1 <= self.apply_startup_quorum <= len(self.node_ids):
            raise ValueError("apply_startup_quorum must be between one and the node count")
        self.apply_armed = self.mode != "apply"
        self.decision_sequence = 0
        self.actuator = actuator
        self.ingest_errors = 0

    def ingest(self, value: str | bytes | dict[str, Any]) -> bool:
        try:
            envelope = parse_envelope(value)
        except ParseError:
            self.ingest_errors += 1
            return False
        if envelope.source_id not in self.states:
            return False
        state = self.states[envelope.source_id]
        ordered, accepted, reset_timing = self.ordering[envelope.source_id].admit(envelope)
        if not accepted:
            return False
        if reset_timing:
            state.frames.clear()
            state.heartbeat = None
            state.last_drop_count = None
            state.last_csi_count = None
            state.output_drop_ratio = 0.0
            state.timing.reset(None, "source-ordering-discontinuity")
            state.timing_estimate = None
            state.pending_timing_events.append(
                {
                    "source_id": envelope.source_id,
                    "event": "source-ordering-discontinuity",
                    "boot_epoch": state.timing.boot_epoch,
                    "ingest_wall_time_ns": envelope.ingest_wall_time_ns,
                }
            )
        if not ordered:
            return True
        handled = False
        for ordered_envelope in ordered:
            if ordered_envelope.parser_status == "malformed":
                state.parse_errors += 1
                continue
            try:
                record = parse_record(ordered_envelope)
            except ParseError:
                state.parse_errors += 1
                continue
            if isinstance(record, Heartbeat):
                state.update_heartbeat(record)
                handled = True
                continue
            if isinstance(record, CsiFrame):
                reported_boot = state.heartbeat.integer("boot_epoch") if state.heartbeat else None
                state.timing_estimate = state.timing.observe(
                    record.local_timestamp_us,
                    record.ingest_monotonic_ns,
                    reported_boot,
                )
                if state.timing_estimate.event:
                    state.pending_timing_events.append(
                        {
                            "source_id": ordered_envelope.source_id,
                            "event": state.timing_estimate.event,
                            "boot_epoch": state.timing_estimate.boot_epoch,
                            "ingest_wall_time_ns": ordered_envelope.ingest_wall_time_ns,
                        }
                    )
                state.frames.append(record)
                handled = True
        return handled

    def _prune(self, now_wall_time_ns: int) -> None:
        cutoff = now_wall_time_ns - int(max(self.window_seconds, self.freshness_seconds) * 1e9 * 2)
        for state in self.states.values():
            while state.frames and state.frames[0].ingest_wall_time_ns < cutoff:
                state.frames.popleft()

    def _evidence(self, source_id: str, now_wall_time_ns: int) -> NodeEvidence:
        state = self.states[source_id]
        cutoff = now_wall_time_ns - int(self.window_seconds * 1e9)
        frames = [frame for frame in state.frames if cutoff <= frame.ingest_wall_time_ns <= now_wall_time_ns]
        good = [frame for frame in frames if frame.rx_state == 0 and not frame.first_word_invalid and frame.csi]
        heartbeat = state.heartbeat
        configured_rate = heartbeat.integer("ping_hz", self.default_rate_hz) if heartbeat else self.default_rate_hz
        expected_frames = max(1.0, float(configured_rate or self.default_rate_hz) * self.window_seconds)
        freshness = None
        if frames:
            freshness = max(0.0, (now_wall_time_ns - frames[-1].ingest_wall_time_ns) / 1e9)
        connected = heartbeat.integer("connected", 1) == 1 if heartbeat else True
        reasons: list[str] = []
        if not connected:
            reasons.append("device-disconnected")
        if freshness is None or freshness > self.freshness_seconds:
            reasons.append("stale-csi")
        if len(good) < self.minimum_frames:
            reasons.append("insufficient-valid-frames")
        timing = state.timing_estimate
        timing_valid = bool(timing and timing.valid)
        if not timing_valid:
            reasons.append("timing-unvalidated")
        temperature_mc = heartbeat.integer("chip_temp_millicelsius") if heartbeat else None
        temperature_c = temperature_mc / 1000.0 if temperature_mc is not None and temperature_mc > -100_000 else None
        if temperature_c is not None and temperature_c >= 80.0:
            reasons.append("thermal-limit")
        rssi_mean, rssi_std = rssi_features(good)
        frequency_spread = frequency_spread_hz(good)
        mean_amp, variance_amp, rms_amp, entropy_amp = amplitude_features(good)
        completeness = clamp(len(good) / expected_frames)
        integrity = len(good) / max(1, len(frames))
        timing_quality = 0.0
        if timing and timing.uncertainty_ms is not None:
            timing_quality = clamp(1.0 - timing.uncertainty_ms / 100.0)
        thermal_quality = 1.0 if temperature_c is None else clamp((80.0 - temperature_c) / 25.0)
        confidence = clamp(
            0.35 * completeness
            + 0.25 * integrity
            + 0.25 * timing_quality
            + 0.15 * thermal_quality
            - state.output_drop_ratio
        )
        availability_blockers = {
            "device-disconnected",
            "stale-csi",
            "insufficient-valid-frames",
            "timing-unvalidated",
            "thermal-limit",
        }
        available = not any(reason in availability_blockers for reason in reasons)
        reliability = confidence if available else 0.0
        csi_cv = (variance_amp**0.5 / mean_amp) if mean_amp > 0.0 else 0.0
        trigger = rssi_std >= self.motion_rssi_std_dbm or csi_cv >= self.motion_csi_cv
        utility = aqmc_utility(rssi_std / self.rssi_std_scale_dbm, frequency_spread, confidence)
        tier = tier_for_utility(utility, trigger)
        motion_score = clamp(0.5 * rssi_std / self.motion_rssi_std_dbm + 0.5 * csi_cv / self.motion_csi_cv)
        return NodeEvidence(
            source_id=source_id,
            available=available,
            unavailable_reasons=tuple(reasons),
            frame_count=len(good),
            expected_frames=expected_frames,
            rssi_mean=rssi_mean,
            rssi_std=rssi_std,
            frequency_spread_hz=frequency_spread,
            csi_mean_amplitude=mean_amp,
            csi_variance=variance_amp,
            csi_rms=rms_amp,
            csi_entropy=entropy_amp,
            confidence=confidence,
            reliability=reliability,
            timing_valid=timing_valid,
            timing_uncertainty_ms=timing.uncertainty_ms if timing else None,
            timing_drift_ppm=timing.drift_ppm if timing else None,
            timing_points=timing.points if timing else 0,
            trigger=trigger,
            utility=utility,
            requested_tier=tier,
            motion_score=motion_score,
            boot_epoch=timing.boot_epoch if timing else None,
            output_drop_ratio=state.output_drop_ratio,
            chip_temperature_c=temperature_c,
        )

    def decide(self, now_wall_time_ns: int | None = None) -> dict[str, Any]:
        now_wall_time_ns = now_wall_time_ns or time.time_ns()
        context_status = "configured-load"
        if self.context_file is not None:
            try:
                context = json.loads(self.context_file.read_text(encoding="utf-8"))
                age = max(0.0, (now_wall_time_ns - int(context["sampled_wall_time_ns"])) / 1e9)
                if age <= self.context_max_age_seconds and context.get("load") in {"light", "moderate", "elevated", "high"}:
                    self.load = str(context["load"])
                    self.context = {**context, "age_seconds": age}
                    context_status = "fresh"
                else:
                    context_status = "stale-or-invalid"
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                context_status = "unavailable"
        prpl_context_status = "not-configured"
        if self.prpl_context_file is not None:
            try:
                prpl_context = json.loads(self.prpl_context_file.read_text(encoding="utf-8"))
                age = max(0.0, (now_wall_time_ns - int(prpl_context["sampled_wall_time_ns"])) / 1e9)
                prpl_load = str(prpl_context.get("load"))
                if age <= self.prpl_context_max_age_seconds and prpl_load in {
                    "light", "moderate", "elevated", "high"
                }:
                    levels = {"light": 0, "moderate": 1, "elevated": 2, "high": 3}
                    if levels[prpl_load] > levels[self.load]:
                        self.load = prpl_load
                    self.prpl_context = {**prpl_context, "age_seconds": age}
                    prpl_context_status = "fresh"
                else:
                    prpl_context_status = "stale-or-invalid"
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                prpl_context_status = "unavailable"
        self._prune(now_wall_time_ns)
        evidence = {source_id: self._evidence(source_id, now_wall_time_ns) for source_id in self.node_ids}
        aqmc = aqmc_admit(evidence, self.load, self.aqmc_parameters)
        decide_policy = aqmc_direct_decide if self.policy_variant == "aqmc-only" else sacm_decide
        sacm = decide_policy(
            self.previous, aqmc.configuration, evidence, self.load,
            self.aqmc_parameters, self.sacm_parameters,
        )
        weights = {source: item.reliability for source, item in evidence.items() if item.available}
        weight_sum = sum(weights.values())
        fused_motion = (
            sum(evidence[source].motion_score * weight for source, weight in weights.items()) / weight_sum
            if weight_sum > 0.0
            else 0.0
        )
        actions: list[dict[str, Any]] = []
        timing_events = [event for state in self.states.values() for event in state.pending_timing_events]
        for state in self.states.values():
            state.pending_timing_events.clear()
        actuation_inhibited = bool(
            self.actuation_inhibit_file is not None and self.actuation_inhibit_file.exists()
        )
        if not self.apply_armed and len(weights) >= self.apply_startup_quorum:
            self.apply_armed = True
        if (
            self.mode == "apply"
            and self.actuator is not None
            and self.apply_armed
            and sacm.configuration != self.last_applied
        ):
            rates = self.aqmc_parameters.rates
            for source_id in self.node_ids:
                previous_tier = self.last_applied.tier(source_id) if self.last_applied else None
                previous_hz = (
                    self.aqmc_parameters.probe_rate_hz
                    if previous_tier == "off"
                    else rates[previous_tier]
                    if previous_tier is not None
                    else None
                )
                desired_tier = sacm.configuration.tier(source_id)
                desired_hz = self.aqmc_parameters.probe_rate_hz if desired_tier == "off" else rates[desired_tier]
                if previous_hz != desired_hz:
                    actions.append(
                        {"source_id": source_id, "hz": desired_hz, "status": "inhibited"}
                        if actuation_inhibited
                        else {
                            "source_id": source_id,
                            "hz": desired_hz,
                            "status": "skipped-unavailable",
                        }
                        if not evidence[source_id].available
                        else self.actuator.apply(source_id, desired_hz)
                    )
            # A desired configuration is confirmed only when every required
            # command ran successfully.  Missing or temporarily unavailable
            # actuators must leave it pending so a later decision can retry.
            if not actuation_inhibited and actions and all(
                action.get("status") == "applied"
                and action.get("verified_committed") is True
                and action.get("schema_version") == ACTUATION_TRANSACTION_V1
                and action.get("transaction_status") == "committed"
                for action in actions
            ):
                self.last_applied = sacm.configuration
        self.previous = sacm.configuration
        self.decision_sequence += 1
        return {
            "schema_version": 1,
            "method": {
                "coordination": "AQ-MC",
                "maintenance": "none" if self.policy_variant == "aqmc-only" else "SACM",
                "features": "CSI-IQ-amplitude-plus-RSSI",
                "timing": self.timing_mode,
                "rate_profile": self.aqmc_parameters.rate_profile,
                "off_tier_probe_rate_hz": self.aqmc_parameters.probe_rate_hz,
            },
            "decision_sequence": self.decision_sequence,
            "decision_wall_time_ns": now_wall_time_ns,
            "load": self.load,
            "network_context": {
                "status": context_status,
                "sample": self.context,
                "prpl_status": prpl_context_status,
                "prpl_sample": self.prpl_context,
                "evidence_transport": {
                    "profile": self.evidence_transport,
                    "listen_host": self.listen_host,
                },
            },
            "mode": self.mode,
            "apply_armed": self.apply_armed,
            "apply_startup_quorum": self.apply_startup_quorum,
            "actuation_inhibited": actuation_inhibited,
            "evidence": json_ready(evidence),
            "aqmc": json_ready(aqmc),
            "sacm": json_ready(sacm),
            "fusion": {"motion_score": fused_motion, "weights": weights, "quorum": len(weights)},
            "timing_events": timing_events,
            "actuation": actions,
            "ingest_errors": self.ingest_errors,
            "parse_errors": {source: state.parse_errors for source, state in self.states.items()},
            "ordering": {
                source: self.ordering[source].ledger() for source in self.node_ids
            },
        }


def load_controller_config(path: Path) -> dict[str, Any]:
    config = json.loads(path.read_text(encoding="utf-8"))
    if config.get("schema_version") != 1:
        raise ValueError("controller schema_version must be 1")
    return config
