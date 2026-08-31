"""Versioned, side-effect-free contracts for the replay reference pipeline.

These records deliberately carry no deployment authority.  They make replay
inputs and decisions auditable without treating a synthetic or replay result as
a physical experiment.
"""
from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any, Mapping


SOURCE_RECORD_V2 = "cws-source-record/2"
EVIDENCE_BUNDLE_V1 = "cws-evidence-bundle/1"
M1_VERDICT_V1 = "cws-m1-verdict/1"
M2_FUSION_RESULT_V1 = "cws-m2-fusion-result/1"
M3_CANDIDATE_RESULT_V1 = "cws-m3-candidate-result/1"
M4_DECISION_V1 = "cws-m4-decision/1"
ACTUATION_TRANSACTION_V1 = "cws-actuation-transaction/1"
ACTUATION_TRANSACTION_V2 = "cws-actuation-transaction/2"

SELF_HASH_FIELD = "sha256"
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
_WIRE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,47}$")
_SOURCE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")
_SESSION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
_REASON = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_UINT32_MAX = (1 << 32) - 1
_KNOWN = {
    SOURCE_RECORD_V2, EVIDENCE_BUNDLE_V1, M1_VERDICT_V1,
    M2_FUSION_RESULT_V1, M3_CANDIDATE_RESULT_V1, M4_DECISION_V1,
    ACTUATION_TRANSACTION_V1, ACTUATION_TRANSACTION_V2,
}


class ContractError(ValueError):
    """A malformed or unverifiable contract fails closed."""


def canonical_json(value: Mapping[str, Any], *, self_hash_field: str = SELF_HASH_FIELD) -> bytes:
    """Return the sole hash encoding: compact, key-sorted UTF-8 JSON.

    Only the top-level self hash is removed.  Nested ``sha256`` values are
    provenance inputs and must remain hash-covered.
    """
    payload = {key: item for key, item in value.items() if key != self_hash_field}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def canonical_sha256(value: Mapping[str, Any], *, self_hash_field: str = SELF_HASH_FIELD) -> str:
    return hashlib.sha256(canonical_json(value, self_hash_field=self_hash_field)).hexdigest()


def _check_version(version: Any, allowed: set[str] | None = None) -> str:
    if not isinstance(version, str) or "/" not in version:
        raise ContractError("invalid-schema-version")
    name, major = version.rsplit("/", 1)
    if not name or not major.isdecimal() or int(major) < 1:
        raise ContractError("invalid-schema-version")
    if allowed is not None and version not in allowed:
        raise ContractError("unknown-major-version")
    return version


def require_id(value: Any, field: str = "id") -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ContractError(f"invalid-{field}")
    return value


def require_reason(value: Any) -> str:
    if not isinstance(value, str) or not _REASON.fullmatch(value):
        raise ContractError("invalid-reason")
    return value


def require_hash(value: Any, field: str = "sha256") -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ContractError(f"invalid-{field}")
    return value


def require_wire_id(value: Any, field: str = "wire-id") -> str:
    if not isinstance(value, str) or not _WIRE_ID.fullmatch(value):
        raise ContractError(f"invalid-{field}")
    return value


def _require_integer(
    value: Any,
    field: str,
    minimum: int,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractError(f"invalid-{field}")
    if maximum is not None and value > maximum:
        raise ContractError(f"invalid-{field}")
    return value


def _require_nullable_hash(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return require_hash(value, field)


def _canonical_data_sha256(value: Mapping[str, Any]) -> str:
    """Hash every field in a collector-owned nested payload."""

    encoded = json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def contract(version: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build a hash-linked contract after validating its public vocabulary."""
    _check_version(version)
    result = deepcopy(dict(payload))
    result["schema_version"] = version
    for field in ("bundle_id", "result_id", "candidate_id", "decision_id", "source_id"):
        if field in result:
            require_id(result[field], field)
    if "reasons" in result:
        if not isinstance(result["reasons"], list):
            raise ContractError("invalid-reasons")
        for reason in result["reasons"]:
            require_reason(reason)
    result[SELF_HASH_FIELD] = canonical_sha256(result)
    return result


def verify_contract(value: Mapping[str, Any], *, versions: set[str], required: tuple[str, ...] = ()) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractError("contract-not-object")
    result = deepcopy(dict(value))
    _check_version(result.get("schema_version"), versions)
    for field in required:
        if field not in result or result[field] is None:
            raise ContractError(f"missing-{field}")
    supplied = require_hash(result.get(SELF_HASH_FIELD))
    if supplied != canonical_sha256(result):
        raise ContractError("hash-mismatch")
    return result


def source_record(value: Mapping[str, Any]) -> dict[str, Any]:
    """Read v1 losslessly enough for admission, normalizing absent v2 facts to None.

    v1 is accepted only as an input compatibility format.  A v1 record is not
    silently promoted: ``source_schema_version`` retains its origin and every
    unavailable fact remains ``None``.
    """
    if not isinstance(value, Mapping):
        raise ContractError("source-record-not-object")
    raw = dict(value)
    if SELF_HASH_FIELD in raw:
        supplied = require_hash(raw[SELF_HASH_FIELD])
        if supplied != canonical_sha256(raw):
            raise ContractError("hash-mismatch")
    version = raw.get("schema_version", "cws-source-record/1")
    _check_version(version)
    if version not in {"cws-source-record/1", SOURCE_RECORD_V2}:
        raise ContractError("unknown-major-version")
    for field in ("source_id", "session_id", "connection_epoch", "source_sequence",
                  "ingest_wall_time_ns", "ingest_monotonic_ns", "raw"):
        if field not in raw:
            raise ContractError(f"missing-{field.replace('_', '-')}")
    if not isinstance(raw["source_id"], str) or not _SOURCE_ID.fullmatch(raw["source_id"]):
        raise ContractError("invalid-source-id")
    if not isinstance(raw["session_id"], str) or not _SESSION_ID.fullmatch(raw["session_id"]):
        raise ContractError("invalid-session-id")
    _require_integer(raw["connection_epoch"], "connection-epoch", 1)
    _require_integer(raw["source_sequence"], "source-sequence", 1)
    _require_integer(raw["ingest_wall_time_ns"], "ingest-wall-time-ns", 0)
    _require_integer(raw["ingest_monotonic_ns"], "ingest-monotonic-ns", 0)
    if not isinstance(raw["raw"], str):
        raise ContractError("invalid-raw")
    if version == SOURCE_RECORD_V2:
        for field in (
            "ingest_sequence", "raw_sha256", "parser_status", "parser_reason",
            "device_sequence", "boot_epoch", "config_epoch",
        ):
            if field not in raw:
                raise ContractError(f"missing-{field.replace('_', '-')}")
        _require_integer(raw["ingest_sequence"], "ingest-sequence", 1)
        require_hash(raw["raw_sha256"], "raw-sha256")
        if raw["raw_sha256"] != hashlib.sha256(raw["raw"].encode("utf-8")).hexdigest():
            raise ContractError("raw-hash-mismatch")
        if raw["parser_status"] not in {"parsed", "unknown", "malformed"}:
            raise ContractError("invalid-parser-status")
        if not isinstance(raw["parser_reason"], str) or not _REASON.fullmatch(raw["parser_reason"]):
            raise ContractError("invalid-parser-reason")
        for field, maximum in (
            ("device_sequence", (1 << 64) - 1),
            ("boot_epoch", _UINT32_MAX),
            ("config_epoch", _UINT32_MAX),
        ):
            item = raw[field]
            if item is not None:
                _require_integer(item, field.replace("_", "-"), 0, maximum)
    facts = raw.get("facts", {})
    if not isinstance(facts, Mapping):
        raise ContractError("invalid-facts")
    derived_raw_hash = hashlib.sha256(raw["raw"].encode("utf-8")).hexdigest()
    v2_value = lambda field: raw.get(field) if version == SOURCE_RECORD_V2 else None
    normalized = {
        "schema_version": SOURCE_RECORD_V2,
        "source_schema_version": version,
        "source_id": raw["source_id"],
        "record_id": raw.get("record_id", raw.get("source_id") + "-record"),
        "raw": raw["raw"],
        "session_id": raw["session_id"], "connection_epoch": raw["connection_epoch"],
        "source_sequence": raw["source_sequence"], "ingest_wall_time_ns": raw["ingest_wall_time_ns"],
        "ingest_monotonic_ns": raw["ingest_monotonic_ns"],
        "ingest_sequence": v2_value("ingest_sequence"),
        "raw_sha256": v2_value("raw_sha256"), "raw_sha256_derived": derived_raw_hash,
        "parser_status": v2_value("parser_status"),
        "parser_reason": v2_value("parser_reason"),
        "device_sequence": v2_value("device_sequence"),
        "boot_epoch": v2_value("boot_epoch"),
        "config_epoch": v2_value("config_epoch"),
        "facts": deepcopy(dict(facts)),
    }
    require_id(normalized["record_id"], "record-id")
    return contract(SOURCE_RECORD_V2, normalized)


def actuation_transaction(
    value: Mapping[str, Any],
    *,
    expected_source_id: str | None = None,
    expected_hz: int | None = None,
    expected_decision_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate the collector's terminal status record without executing it."""

    if not isinstance(value, Mapping):
        raise ContractError("actuation-transaction-not-object")
    payload = deepcopy(dict(value))
    required_top = {
        "schema_version", "command_id", "transaction_id", "source_id", "run_id",
        "hz", "status", "transaction_status", "reason", "responded_wall_time_ns",
        "terminal_facts", "terminal_facts_sha256",
    }
    missing = sorted(required_top - payload.keys())
    if missing:
        raise ContractError(f"missing-{missing[0].replace('_', '-')}")
    version = payload["schema_version"]
    if version not in {ACTUATION_TRANSACTION_V1, ACTUATION_TRANSACTION_V2}:
        raise ContractError("unknown-major-version")
    if expected_decision_sha256 is not None:
        require_hash(expected_decision_sha256, "expected-decision-sha256")
        if version == ACTUATION_TRANSACTION_V1:
            raise ContractError("decision-link-required")
    decision_sha256: str | None = None
    if version == ACTUATION_TRANSACTION_V2:
        if "decision_sha256" not in payload:
            raise ContractError("missing-decision-sha256")
        decision_sha256 = require_hash(payload["decision_sha256"], "decision-sha256")
        if (
            expected_decision_sha256 is not None
            and decision_sha256 != expected_decision_sha256
        ):
            raise ContractError("expected-decision-mismatch")
    for field in ("command_id", "transaction_id"):
        require_wire_id(payload[field], field.replace("_", "-"))
    if payload["command_id"] != payload["transaction_id"]:
        raise ContractError("transaction-correlation-mismatch")
    if not isinstance(payload["source_id"], str) or not _SOURCE_ID.fullmatch(payload["source_id"]):
        raise ContractError("invalid-source-id")
    require_id(payload["run_id"], "run-id")
    _require_integer(payload["hz"], "hz", 0, 50)
    _require_integer(payload["responded_wall_time_ns"], "responded-wall-time-ns", 0)
    if not isinstance(payload["reason"], str) or not _REASON.fullmatch(payload["reason"]):
        raise ContractError("invalid-reason")
    if expected_source_id is not None and payload["source_id"] != expected_source_id:
        raise ContractError("source-mismatch")
    if expected_hz is not None and payload["hz"] != expected_hz:
        raise ContractError("rate-mismatch")
    terminal = payload["transaction_status"]
    if terminal not in {"committed", "rolled-back", "failed-safe", "rollback-failed"}:
        raise ContractError("invalid-terminal-status")
    expected_status = "applied" if terminal == "committed" else "rejected"
    if payload["status"] != expected_status:
        raise ContractError("terminal-status-mismatch")

    facts = payload["terminal_facts"]
    if not isinstance(facts, Mapping):
        raise ContractError("missing-terminal-facts")
    facts = deepcopy(dict(facts))
    required_facts = {
        "requested_state_sha256", "epoch_bound_requested_state_sha256",
        "pre_state_sha256", "applied_state_sha256", "restored_state_sha256",
        "apply_observed_state_sha256", "restore_observed_state_sha256",
        "phase_command_ids", "phase_deadline_wall_time_ns", "correlated_replies",
        "postcondition", "rollback",
    }
    missing = sorted(required_facts - facts.keys())
    if missing:
        raise ContractError(f"missing-{missing[0].replace('_', '-')}")
    if version == ACTUATION_TRANSACTION_V2:
        if "decision_sha256" not in facts:
            raise ContractError("missing-terminal-decision-sha256")
        terminal_decision_sha256 = require_hash(
            facts["decision_sha256"], "terminal-decision-sha256"
        )
        if terminal_decision_sha256 != decision_sha256:
            raise ContractError("decision-link-mismatch")
    supplied_facts_hash = require_hash(
        payload["terminal_facts_sha256"], "terminal-facts-sha256"
    )
    try:
        actual_facts_hash = _canonical_data_sha256(facts)
    except (TypeError, ValueError) as exc:
        raise ContractError("invalid-terminal-facts") from exc
    if supplied_facts_hash != actual_facts_hash:
        raise ContractError("terminal-facts-hash-mismatch")
    hashes = {
        field: _require_nullable_hash(facts[field], field.replace("_", "-"))
        for field in (
            "requested_state_sha256", "epoch_bound_requested_state_sha256",
            "pre_state_sha256", "applied_state_sha256", "restored_state_sha256",
            "apply_observed_state_sha256", "restore_observed_state_sha256",
        )
    }
    requested_hash = hashes["requested_state_sha256"]
    if requested_hash is not None:
        request_facts = {
            "schema_version": (
                "cws-firmware-state-request/2"
                if version == ACTUATION_TRANSACTION_V2
                else "cws-firmware-state-request/1"
            ),
            "source_id": payload["source_id"],
            "transaction_id": payload["transaction_id"],
            "requested_ping_hz": payload["hz"],
        }
        if version == ACTUATION_TRANSACTION_V2:
            request_facts["decision_sha256"] = decision_sha256
        expected_request = _canonical_data_sha256(request_facts)
        if requested_hash != expected_request:
            raise ContractError("requested-state-correlation-mismatch")

    phases = facts["phase_command_ids"]
    deadlines = facts["phase_deadline_wall_time_ns"]
    if not isinstance(phases, Mapping):
        raise ContractError("missing-phase-command-ids")
    if not isinstance(deadlines, Mapping) or set(deadlines) != set(phases):
        raise ContractError("phase-deadline-mismatch")
    allowed_phases = {
        "state-before", "prepare", "apply", "verify", "recover-state",
        "restore", "restore-verify",
    }
    for phase, command_id in phases.items():
        if not isinstance(phase, str) or not (
            phase in allowed_phases
            or (
                phase.startswith("query-")
                and phase.removeprefix("query-").isdecimal()
                and int(phase.removeprefix("query-")) >= 1
            )
        ):
            raise ContractError("invalid-phase")
        require_wire_id(command_id, "phase-command-id")
        _require_integer(deadlines[phase], "phase-deadline", 1)
    if len(set(phases.values())) != len(phases):
        raise ContractError("duplicate-phase-command-id")

    replies = facts["correlated_replies"]
    if not isinstance(replies, list):
        raise ContractError("missing-correlated-replies")
    reply_phases: set[str] = set()
    reply_commands: set[str] = set()
    query_commands = {command_id for phase, command_id in phases.items() if phase.startswith("query-")}
    for reply in replies:
        if not isinstance(reply, Mapping):
            raise ContractError("invalid-correlated-reply")
        phase = reply.get("phase")
        command_id = reply.get("command_id")
        if phase == "query":
            correlated = command_id in query_commands
        else:
            correlated = phase in phases and phases[phase] == command_id
        if not correlated:
            raise ContractError("reply-phase-correlation-mismatch")
        if command_id in reply_commands:
            raise ContractError("duplicate-correlated-reply")
        reply_commands.add(command_id)
        reply_phases.add(phase)
        require_hash(reply.get("reply_sha256"), "reply-sha256")

    postcondition = facts["postcondition"]
    if postcondition is not None:
        if not isinstance(postcondition, Mapping) or not isinstance(postcondition.get("verified"), bool):
            raise ContractError("invalid-postcondition")
        if not isinstance(postcondition.get("reason"), str) or not _REASON.fullmatch(postcondition["reason"]):
            raise ContractError("invalid-postcondition-reason")
        require_hash(postcondition.get("state_sha256"), "postcondition-state-sha256")
    rollback = facts["rollback"]
    if not isinstance(rollback, Mapping) or not isinstance(rollback.get("requested"), bool):
        raise ContractError("invalid-rollback")
    if "verified" in rollback and not isinstance(rollback["verified"], bool):
        raise ContractError("invalid-rollback")
    if rollback.get("requested") is False and rollback.get("verified") is True:
        raise ContractError("invalid-rollback")
    if rollback.get("reason") is not None and (
        not isinstance(rollback["reason"], str) or not _REASON.fullmatch(rollback["reason"])
    ):
        raise ContractError("invalid-rollback-reason")
    if rollback.get("requested") is True:
        _require_integer(rollback.get("prior_ping_hz"), "rollback-prior-ping-hz", 0, 50)
        _require_integer(
            rollback.get("expected_config_epoch"),
            "rollback-expected-config-epoch",
            0,
            _UINT32_MAX,
        )
    if rollback.get("state_sha256") is not None:
        require_hash(rollback["state_sha256"], "rollback-state-sha256")
    if rollback.get("observed_state_sha256") is not None:
        require_hash(rollback["observed_state_sha256"], "rollback-observed-state-sha256")

    required_commit_hashes = (
        "requested_state_sha256", "epoch_bound_requested_state_sha256",
        "pre_state_sha256", "applied_state_sha256",
    )
    if terminal == "committed":
        if any(hashes[field] is None for field in required_commit_hashes):
            raise ContractError("missing-committed-state-hash")
        if hashes["restored_state_sha256"] is not None or hashes["restore_observed_state_sha256"] is not None:
            raise ContractError("invalid-committed-restore-evidence")
        if rollback["requested"] is not False:
            raise ContractError("invalid-committed-evidence")
        if postcondition is None or postcondition["verified"] is not True:
            raise ContractError("postcondition-unverified")
        if postcondition["state_sha256"] != hashes["applied_state_sha256"]:
            raise ContractError("postcondition-state-mismatch")
        base_phases = {"state-before", "prepare", "apply"}
        if not base_phases.issubset(phases):
            raise ContractError("missing-required-phase")
        normal = "verify" in phases and "recover-state" not in phases
        recovered = "recover-state" in phases and "verify" not in phases
        if normal:
            allowed = base_phases | {"verify"} | {
                phase for phase in phases if phase.startswith("query-")
            }
            if set(phases) != allowed:
                raise ContractError("invalid-normal-phase-set")
            if payload["reason"] != "postcondition-verified" or postcondition["reason"] != "postcondition-verified":
                raise ContractError("invalid-normal-commit-reason")
            if not {"state-before", "prepare", "apply", "verify"}.issubset(reply_phases):
                raise ContractError("incomplete-correlated-replies")
            if hashes["apply_observed_state_sha256"] != hashes["applied_state_sha256"]:
                raise ContractError("apply-state-hash-mismatch")
        elif recovered:
            allowed = base_phases | {"recover-state"} | {
                phase for phase in phases if phase.startswith("query-")
            }
            if set(phases) != allowed or "query-1" not in phases:
                raise ContractError("invalid-recovered-phase-set")
            if payload["reason"] != "postcondition-recovered" or postcondition["reason"] != "postcondition-recovered":
                raise ContractError("invalid-recovered-commit-reason")
            if not {"state-before", "prepare", "recover-state"}.issubset(reply_phases):
                raise ContractError("incomplete-recovery-replies")
            if "apply" in reply_phases or "query" in reply_phases or reply_commands.intersection(query_commands):
                raise ContractError("invalid-recovered-reply-evidence")
            if hashes["apply_observed_state_sha256"] is not None:
                raise ContractError("invalid-recovered-apply-evidence")
        else:
            raise ContractError("invalid-committed-phase-path")
    elif terminal == "rolled-back":
        if (
            hashes["requested_state_sha256"] is None
            or hashes["pre_state_sha256"] is None
            or hashes["restored_state_sha256"] is None
            or rollback["requested"] is not True
            or rollback.get("verified") is not True
            or rollback.get("state_sha256") != hashes["restored_state_sha256"]
            or "restore" not in phases
        ):
            raise ContractError("invalid-rollback-evidence")
    elif terminal == "rollback-failed":
        if rollback.get("verified") is True or (postcondition is not None and postcondition["verified"] is True):
            raise ContractError("invalid-rollback-failure-evidence")
    else:  # failed-safe: no applied commit and no rollback action to conceal it.
        if (
            hashes["applied_state_sha256"] is not None
            or hashes["restored_state_sha256"] is not None
            or rollback["requested"] is not False
            or (postcondition is not None and postcondition["verified"] is True)
        ):
            raise ContractError("invalid-failed-safe-evidence")
    return payload
