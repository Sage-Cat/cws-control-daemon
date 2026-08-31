"""Versioned, backend-neutral contracts for future local actuator adapters.

This module provides no OpenWrt, prplMesh, EasyMesh, command, network, or
hardware implementation. Operation identifiers and all backend data remain
opaque. The runner only sequences an explicitly supplied in-process adapter.
"""
from __future__ import annotations

import hashlib
import json
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Collection, Mapping, Protocol

from cws_control.contracts import (
    ContractError,
    contract,
    require_hash,
    require_id,
    verify_contract,
)


ADAPTER_CAPABILITIES_V1 = "cws-adapter-capabilities/1"
ADAPTER_REQUEST_V1 = "cws-adapter-request/1"
ADAPTER_TRANSACTION_V1 = "cws-adapter-transaction/1"
MAX_ADAPTER_TIMEOUT_MS = 30_000
MAX_OPAQUE_BYTES = 1_048_576
MAX_OPERATIONS = 128

BACKEND_KINDS = frozenset({"openwrt", "prplmesh", "easymesh"})
ACTION_BOUNDARIES = frozenset({"measurement-control", "traffic-topology"})
REQUEST_MODES = frozenset({"observe", "dry-run", "apply"})
TRANSACTION_STATUSES = frozenset(
    {"observed", "dry-run", "applied", "rejected", "failed", "rolled-back", "rollback-failed"}
)
TRANSACTION_REASONS = frozenset(
    {
        "observe-complete",
        "dry-run-complete",
        "apply-verified",
        "apply-latch-required",
        "apply-unsupported",
        "adapter-id-mismatch",
        "operation-not-allowed",
        "operation-not-supported",
        "boundary-mismatch",
        "timeout-exceeds-capability",
        "stale-precondition",
        "adapter-timeout",
        "adapter-deadline-exceeded",
        "adapter-error",
        "postcondition-mismatch",
    }
)
PHASES = frozenset({"read-precondition", "preview", "apply", "read-postcondition", "rollback", "read-rollback"})
PHASE_STATUSES = frozenset({"ok", "error", "timeout", "deadline-exceeded"})

_CAPABILITY_FIELDS = frozenset(
    {"schema_version", "adapter_id", "backend_kind", "operations", "max_timeout_ms", "sha256"}
)
_OPERATION_FIELDS = frozenset({"operation_id", "action_boundary", "apply_supported"})
_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "adapter_id",
        "operation_id",
        "action_boundary",
        "mode",
        "parameters",
        "parameters_sha256",
        "expected_pre_state_sha256",
        "expected_post_state_sha256",
        "timeout_ms",
        "sha256",
    }
)
_TRANSACTION_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_id",
        "adapter_id",
        "backend_kind",
        "operation_id",
        "action_boundary",
        "mode",
        "status",
        "reason",
        "capabilities_sha256",
        "request_sha256",
        "parameters_sha256",
        "expected_pre_state_sha256",
        "expected_post_state_sha256",
        "pre_state_sha256",
        "post_state_sha256",
        "rollback_state_sha256",
        "adapter_result_sha256",
        "timeout_ms",
        "phase_ledger",
        "sha256",
    }
)
_PHASE_FIELDS = frozenset({"phase", "status", "deadline_monotonic_ns", "result_sha256"})


def _exact_fields(value: Mapping[str, Any], expected: frozenset[str], label: str) -> None:
    fields = set(value)
    if fields != expected:
        if expected - fields:
            raise ContractError(f"missing-{label}-field")
        raise ContractError(f"unknown-{label}-field")


def _integer(value: Any, label: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractError(f"invalid-{label}")
    if maximum is not None and value > maximum:
        raise ContractError(f"invalid-{label}")
    return value


def _nullable_hash(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return require_hash(value, label)


def _opaque_bytes(value: Mapping[str, Any]) -> bytes:
    if not isinstance(value, Mapping):
        raise ContractError("opaque-data-not-object")
    try:
        encoded = json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ContractError("invalid-opaque-data") from exc
    if len(encoded) > MAX_OPAQUE_BYTES:
        raise ContractError("opaque-data-too-large")
    return encoded


def opaque_sha256(value: Mapping[str, Any]) -> str:
    """Hash every byte-significant field of adapter-owned opaque data."""

    return hashlib.sha256(_opaque_bytes(value)).hexdigest()


def make_adapter_capabilities(
    *,
    adapter_id: str,
    backend_kind: str,
    operations: list[Mapping[str, Any]],
    max_timeout_ms: int,
) -> dict[str, Any]:
    return validate_adapter_capabilities(
        contract(
            ADAPTER_CAPABILITIES_V1,
            {
                "adapter_id": adapter_id,
                "backend_kind": backend_kind,
                "operations": [dict(item) for item in operations],
                "max_timeout_ms": max_timeout_ms,
            },
        )
    )


def validate_adapter_capabilities(value: Mapping[str, Any]) -> dict[str, Any]:
    result = verify_contract(value, versions={ADAPTER_CAPABILITIES_V1})
    _exact_fields(result, _CAPABILITY_FIELDS, "capabilities")
    require_id(result["adapter_id"], "adapter-id")
    if result["backend_kind"] not in BACKEND_KINDS:
        raise ContractError("unknown-backend-kind")
    _integer(result["max_timeout_ms"], "max-timeout-ms", 1, MAX_ADAPTER_TIMEOUT_MS)
    operations = result["operations"]
    if not isinstance(operations, list) or not 1 <= len(operations) <= MAX_OPERATIONS:
        raise ContractError("invalid-operations")
    identifiers: set[str] = set()
    for item in operations:
        if not isinstance(item, Mapping):
            raise ContractError("invalid-operation-capability")
        _exact_fields(item, _OPERATION_FIELDS, "operation-capability")
        operation_id = require_id(item["operation_id"], "operation-id")
        if operation_id in identifiers:
            raise ContractError("duplicate-operation-id")
        identifiers.add(operation_id)
        if item["action_boundary"] not in ACTION_BOUNDARIES:
            raise ContractError("unknown-action-boundary")
        if not isinstance(item["apply_supported"], bool):
            raise ContractError("invalid-apply-supported")
    return result


def make_adapter_request(
    *,
    request_id: str,
    adapter_id: str,
    operation_id: str,
    action_boundary: str,
    parameters: Mapping[str, Any],
    timeout_ms: int,
    mode: str = "observe",
    expected_pre_state_sha256: str | None = None,
    expected_post_state_sha256: str | None = None,
) -> dict[str, Any]:
    if not isinstance(parameters, Mapping):
        raise ContractError("opaque-data-not-object")
    parameter_values = deepcopy(dict(parameters))
    return validate_adapter_request(
        contract(
            ADAPTER_REQUEST_V1,
            {
                "request_id": request_id,
                "adapter_id": adapter_id,
                "operation_id": operation_id,
                "action_boundary": action_boundary,
                "mode": mode,
                "parameters": parameter_values,
                "parameters_sha256": opaque_sha256(parameter_values),
                "expected_pre_state_sha256": expected_pre_state_sha256,
                "expected_post_state_sha256": expected_post_state_sha256,
                "timeout_ms": timeout_ms,
            },
        )
    )


def validate_adapter_request(value: Mapping[str, Any]) -> dict[str, Any]:
    result = verify_contract(value, versions={ADAPTER_REQUEST_V1})
    _exact_fields(result, _REQUEST_FIELDS, "request")
    require_id(result["request_id"], "request-id")
    require_id(result["adapter_id"], "adapter-id")
    require_id(result["operation_id"], "operation-id")
    if result["action_boundary"] not in ACTION_BOUNDARIES:
        raise ContractError("unknown-action-boundary")
    if result["mode"] not in REQUEST_MODES:
        raise ContractError("unknown-request-mode")
    _integer(result["timeout_ms"], "timeout-ms", 1, MAX_ADAPTER_TIMEOUT_MS)
    supplied_parameters_hash = require_hash(result["parameters_sha256"], "parameters-sha256")
    if supplied_parameters_hash != opaque_sha256(result["parameters"]):
        raise ContractError("parameters-hash-mismatch")
    expected_pre = _nullable_hash(result["expected_pre_state_sha256"], "expected-pre-state-sha256")
    expected_post = _nullable_hash(result["expected_post_state_sha256"], "expected-post-state-sha256")
    if result["mode"] == "observe":
        if expected_pre is not None or expected_post is not None:
            raise ContractError("observe-state-expectation-forbidden")
    elif expected_pre is None or expected_post is None:
        raise ContractError("missing-state-expectation")
    return result


def validate_adapter_transaction(value: Mapping[str, Any]) -> dict[str, Any]:
    result = verify_contract(value, versions={ADAPTER_TRANSACTION_V1})
    _exact_fields(result, _TRANSACTION_FIELDS, "transaction")
    require_id(result["transaction_id"], "transaction-id")
    require_id(result["adapter_id"], "adapter-id")
    require_id(result["operation_id"], "operation-id")
    if result["backend_kind"] not in BACKEND_KINDS:
        raise ContractError("unknown-backend-kind")
    if result["action_boundary"] not in ACTION_BOUNDARIES:
        raise ContractError("unknown-action-boundary")
    if result["mode"] not in REQUEST_MODES:
        raise ContractError("unknown-request-mode")
    if result["status"] not in TRANSACTION_STATUSES:
        raise ContractError("unknown-transaction-status")
    if result["reason"] not in TRANSACTION_REASONS:
        raise ContractError("unknown-transaction-reason")
    for field in ("capabilities_sha256", "request_sha256", "parameters_sha256"):
        require_hash(result[field], field.replace("_", "-"))
    for field in (
        "expected_pre_state_sha256",
        "expected_post_state_sha256",
        "pre_state_sha256",
        "post_state_sha256",
        "rollback_state_sha256",
        "adapter_result_sha256",
    ):
        _nullable_hash(result[field], field.replace("_", "-"))
    _integer(result["timeout_ms"], "timeout-ms", 1, MAX_ADAPTER_TIMEOUT_MS)
    ledger = result["phase_ledger"]
    if not isinstance(ledger, list) or len(ledger) > 8:
        raise ContractError("invalid-phase-ledger")
    for item in ledger:
        if not isinstance(item, Mapping):
            raise ContractError("invalid-phase-entry")
        _exact_fields(item, _PHASE_FIELDS, "phase")
        if item["phase"] not in PHASES:
            raise ContractError("unknown-phase")
        if item["status"] not in PHASE_STATUSES:
            raise ContractError("unknown-phase-status")
        _integer(item["deadline_monotonic_ns"], "deadline-monotonic-ns", 1)
        _nullable_hash(item["result_sha256"], "phase-result-sha256")
    phases = [item["phase"] for item in ledger]
    if len(phases) != len(set(phases)):
        raise ContractError("duplicate-phase")
    by_phase = {item["phase"]: item for item in ledger}
    status = result["status"]
    reason = result["reason"]
    if status == "observed" and not (
        result["mode"] == "observe"
        and reason == "observe-complete"
        and phases == ["read-precondition"]
        and result["expected_pre_state_sha256"] is None
        and result["expected_post_state_sha256"] is None
        and result["pre_state_sha256"] is not None
        and result["post_state_sha256"] == result["pre_state_sha256"]
        and by_phase.get("read-precondition", {}).get("status") == "ok"
        and by_phase.get("read-precondition", {}).get("result_sha256") == result["pre_state_sha256"]
    ):
        raise ContractError("invalid-observed-transaction")
    if status == "dry-run" and not (
        result["mode"] == "dry-run"
        and reason == "dry-run-complete"
        and phases == ["read-precondition", "preview"]
        and result["expected_pre_state_sha256"] is not None
        and result["expected_post_state_sha256"] is not None
        and result["pre_state_sha256"] == result["expected_pre_state_sha256"]
        and result["adapter_result_sha256"] is not None
        and result["post_state_sha256"] is None
        and result["rollback_state_sha256"] is None
        and all(item["status"] == "ok" for item in ledger)
        and by_phase.get("read-precondition", {}).get("result_sha256") == result["pre_state_sha256"]
        and by_phase.get("preview", {}).get("result_sha256") == result["adapter_result_sha256"]
    ):
        raise ContractError("invalid-dry-run-transaction")
    if status == "applied" and not (
        result["mode"] == "apply"
        and reason == "apply-verified"
        and phases == ["read-precondition", "apply", "read-postcondition"]
        and result["expected_pre_state_sha256"] is not None
        and result["expected_post_state_sha256"] is not None
        and result["pre_state_sha256"] == result["expected_pre_state_sha256"]
        and result["post_state_sha256"] == result["expected_post_state_sha256"]
        and result["adapter_result_sha256"] is not None
        and result["rollback_state_sha256"] is None
        and all(item["status"] == "ok" for item in ledger)
        and by_phase.get("read-precondition", {}).get("result_sha256") == result["pre_state_sha256"]
        and by_phase.get("apply", {}).get("result_sha256") == result["adapter_result_sha256"]
        and by_phase.get("read-postcondition", {}).get("result_sha256") == result["post_state_sha256"]
    ):
        raise ContractError("invalid-applied-transaction")
    recovery_reason = reason in {
        "adapter-timeout",
        "adapter-deadline-exceeded",
        "adapter-error",
        "postcondition-mismatch",
    }
    rejection_reason = reason in {
        "apply-latch-required",
        "apply-unsupported",
        "adapter-id-mismatch",
        "operation-not-allowed",
        "operation-not-supported",
        "boundary-mismatch",
        "timeout-exceeds-capability",
        "stale-precondition",
    }
    if status == "rejected" and not rejection_reason:
        raise ContractError("invalid-rejected-transaction")
    if status == "failed" and not (
        reason in {"adapter-timeout", "adapter-deadline-exceeded", "adapter-error"}
        and phases
        and ledger[-1]["status"] != "ok"
    ):
        raise ContractError("invalid-failed-transaction")
    if status == "rolled-back" and not (
        result["mode"] == "apply"
        and recovery_reason
        and result["expected_pre_state_sha256"] is not None
        and result["expected_post_state_sha256"] is not None
        and result["pre_state_sha256"] == result["expected_pre_state_sha256"]
        and result["pre_state_sha256"] is not None
        and result["rollback_state_sha256"] == result["pre_state_sha256"]
        and phases[-2:] == ["rollback", "read-rollback"]
        and by_phase.get("read-precondition", {}).get("status") == "ok"
        and by_phase.get("read-precondition", {}).get("result_sha256") == result["pre_state_sha256"]
        and by_phase.get("read-rollback", {}).get("status") == "ok"
        and by_phase.get("read-rollback", {}).get("result_sha256") == result["rollback_state_sha256"]
    ):
        raise ContractError("invalid-rolled-back-transaction")
    if status == "rollback-failed" and not (
        result["mode"] == "apply"
        and recovery_reason
        and result["expected_pre_state_sha256"] is not None
        and result["expected_post_state_sha256"] is not None
        and result["pre_state_sha256"] == result["expected_pre_state_sha256"]
        and result["pre_state_sha256"] is not None
        and result["rollback_state_sha256"] != result["pre_state_sha256"]
        and phases[-2:] == ["rollback", "read-rollback"]
        and by_phase.get("read-precondition", {}).get("status") == "ok"
        and by_phase.get("read-precondition", {}).get("result_sha256") == result["pre_state_sha256"]
    ):
        raise ContractError("invalid-rollback-failed-transaction")
    return result


@dataclass(frozen=True)
class AdapterCallContext:
    timeout_ms: int
    deadline_monotonic_ns: int


class Adapter(Protocol):
    """In-process interface only; implementations must honor call deadlines."""

    def capabilities(self) -> Mapping[str, Any]: ...

    def read_state(
        self,
        operation_id: str,
        parameters: Mapping[str, Any],
        context: AdapterCallContext,
    ) -> Mapping[str, Any]: ...

    def preview(
        self,
        operation_id: str,
        parameters: Mapping[str, Any],
        context: AdapterCallContext,
    ) -> Mapping[str, Any]: ...

    def apply(
        self,
        operation_id: str,
        parameters: Mapping[str, Any],
        context: AdapterCallContext,
    ) -> Mapping[str, Any]: ...

    def rollback(
        self,
        operation_id: str,
        captured_state: Mapping[str, Any],
        context: AdapterCallContext,
    ) -> Mapping[str, Any]: ...


class _AdapterCallFailure(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class AdapterRunner:
    """Fail-closed sequencer; ``allow_apply`` is only a local software latch."""

    def __init__(
        self,
        adapter: Adapter,
        *,
        allowed_operation_ids: Collection[str],
        allow_apply: bool = False,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ):
        if isinstance(allowed_operation_ids, (str, bytes)):
            raise ContractError("invalid-operation-allowlist")
        identifiers = [require_id(item, "allowed-operation-id") for item in allowed_operation_ids]
        if len(identifiers) != len(set(identifiers)):
            raise ContractError("duplicate-allowed-operation-id")
        if not isinstance(allow_apply, bool):
            raise ContractError("invalid-allow-apply-latch")
        self.adapter = adapter
        self.allowed_operation_ids = frozenset(identifiers)
        self.allow_apply = allow_apply
        self.monotonic_ns = monotonic_ns

    def _call(
        self,
        ledger: list[dict[str, Any]],
        phase: str,
        timeout_ms: int,
        function: Callable[[AdapterCallContext], Mapping[str, Any]],
    ) -> tuple[dict[str, Any], str]:
        deadline = self.monotonic_ns() + timeout_ms * 1_000_000
        context = AdapterCallContext(timeout_ms=timeout_ms, deadline_monotonic_ns=deadline)
        try:
            value = function(context)
            digest = opaque_sha256(value)
        except TimeoutError as exc:
            ledger.append({"phase": phase, "status": "timeout", "deadline_monotonic_ns": deadline, "result_sha256": None})
            raise _AdapterCallFailure("adapter-timeout") from exc
        except Exception as exc:
            ledger.append({"phase": phase, "status": "error", "deadline_monotonic_ns": deadline, "result_sha256": None})
            raise _AdapterCallFailure("adapter-error") from exc
        if self.monotonic_ns() > deadline:
            ledger.append({"phase": phase, "status": "deadline-exceeded", "deadline_monotonic_ns": deadline, "result_sha256": digest})
            raise _AdapterCallFailure("adapter-deadline-exceeded")
        ledger.append({"phase": phase, "status": "ok", "deadline_monotonic_ns": deadline, "result_sha256": digest})
        return deepcopy(dict(value)), digest

    def _transaction(
        self,
        capabilities: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        status: str,
        reason: str,
        ledger: list[dict[str, Any]],
        pre_state_sha256: str | None = None,
        post_state_sha256: str | None = None,
        rollback_state_sha256: str | None = None,
        adapter_result_sha256: str | None = None,
    ) -> dict[str, Any]:
        return validate_adapter_transaction(
            contract(
                ADAPTER_TRANSACTION_V1,
                {
                    "transaction_id": request["request_id"],
                    "adapter_id": request["adapter_id"],
                    "backend_kind": capabilities["backend_kind"],
                    "operation_id": request["operation_id"],
                    "action_boundary": request["action_boundary"],
                    "mode": request["mode"],
                    "status": status,
                    "reason": reason,
                    "capabilities_sha256": capabilities["sha256"],
                    "request_sha256": request["sha256"],
                    "parameters_sha256": request["parameters_sha256"],
                    "expected_pre_state_sha256": request["expected_pre_state_sha256"],
                    "expected_post_state_sha256": request["expected_post_state_sha256"],
                    "pre_state_sha256": pre_state_sha256,
                    "post_state_sha256": post_state_sha256,
                    "rollback_state_sha256": rollback_state_sha256,
                    "adapter_result_sha256": adapter_result_sha256,
                    "timeout_ms": request["timeout_ms"],
                    "phase_ledger": ledger,
                },
            )
        )

    def run(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_adapter_request(value)
        capabilities = validate_adapter_capabilities(self.adapter.capabilities())
        ledger: list[dict[str, Any]] = []
        operation_id = request["operation_id"]
        if request["adapter_id"] != capabilities["adapter_id"]:
            return self._transaction(capabilities, request, status="rejected", reason="adapter-id-mismatch", ledger=ledger)
        if operation_id not in self.allowed_operation_ids:
            return self._transaction(capabilities, request, status="rejected", reason="operation-not-allowed", ledger=ledger)
        operation = next(
            (item for item in capabilities["operations"] if item["operation_id"] == operation_id),
            None,
        )
        if operation is None:
            return self._transaction(capabilities, request, status="rejected", reason="operation-not-supported", ledger=ledger)
        if operation["action_boundary"] != request["action_boundary"]:
            return self._transaction(capabilities, request, status="rejected", reason="boundary-mismatch", ledger=ledger)
        if request["timeout_ms"] > capabilities["max_timeout_ms"]:
            return self._transaction(capabilities, request, status="rejected", reason="timeout-exceeds-capability", ledger=ledger)
        if request["mode"] == "apply":
            if operation["apply_supported"] is not True:
                return self._transaction(capabilities, request, status="rejected", reason="apply-unsupported", ledger=ledger)
            if self.allow_apply is not True:
                return self._transaction(capabilities, request, status="rejected", reason="apply-latch-required", ledger=ledger)

        timeout_ms = request["timeout_ms"]
        parameters = request["parameters"]
        try:
            pre_state, pre_hash = self._call(
                ledger,
                "read-precondition",
                timeout_ms,
                lambda context: self.adapter.read_state(operation_id, deepcopy(parameters), context),
            )
        except _AdapterCallFailure as exc:
            return self._transaction(capabilities, request, status="failed", reason=exc.reason, ledger=ledger)

        if request["mode"] == "observe":
            return self._transaction(
                capabilities,
                request,
                status="observed",
                reason="observe-complete",
                ledger=ledger,
                pre_state_sha256=pre_hash,
                post_state_sha256=pre_hash,
            )
        if pre_hash != request["expected_pre_state_sha256"]:
            return self._transaction(
                capabilities,
                request,
                status="rejected",
                reason="stale-precondition",
                ledger=ledger,
                pre_state_sha256=pre_hash,
            )
        if request["mode"] == "dry-run":
            try:
                _preview, preview_hash = self._call(
                    ledger,
                    "preview",
                    timeout_ms,
                    lambda context: self.adapter.preview(operation_id, deepcopy(parameters), context),
                )
            except _AdapterCallFailure as exc:
                return self._transaction(
                    capabilities,
                    request,
                    status="failed",
                    reason=exc.reason,
                    ledger=ledger,
                    pre_state_sha256=pre_hash,
                )
            return self._transaction(
                capabilities,
                request,
                status="dry-run",
                reason="dry-run-complete",
                ledger=ledger,
                pre_state_sha256=pre_hash,
                adapter_result_sha256=preview_hash,
            )

        failure_reason: str | None = None
        adapter_result_sha256: str | None = None
        post_hash: str | None = None
        try:
            _adapter_result, adapter_result_sha256 = self._call(
                ledger,
                "apply",
                timeout_ms,
                lambda context: self.adapter.apply(operation_id, deepcopy(parameters), context),
            )
            _post_state, post_hash = self._call(
                ledger,
                "read-postcondition",
                timeout_ms,
                lambda context: self.adapter.read_state(operation_id, deepcopy(parameters), context),
            )
            if post_hash != request["expected_post_state_sha256"]:
                failure_reason = "postcondition-mismatch"
        except _AdapterCallFailure as exc:
            failure_reason = exc.reason
        if failure_reason is None:
            return self._transaction(
                capabilities,
                request,
                status="applied",
                reason="apply-verified",
                ledger=ledger,
                pre_state_sha256=pre_hash,
                post_state_sha256=post_hash,
                adapter_result_sha256=adapter_result_sha256,
            )

        try:
            self._call(
                ledger,
                "rollback",
                timeout_ms,
                lambda context: self.adapter.rollback(operation_id, pre_state, context),
            )
        except _AdapterCallFailure:
            pass
        rollback_hash: str | None = None
        try:
            _rollback_state, rollback_hash = self._call(
                ledger,
                "read-rollback",
                timeout_ms,
                lambda context: self.adapter.read_state(operation_id, deepcopy(parameters), context),
            )
        except _AdapterCallFailure:
            pass
        rolled_back = rollback_hash == pre_hash
        return self._transaction(
            capabilities,
            request,
            status="rolled-back" if rolled_back else "rollback-failed",
            reason=failure_reason,
            ledger=ledger,
            pre_state_sha256=pre_hash,
            post_state_sha256=post_hash,
            rollback_state_sha256=rollback_hash,
            adapter_result_sha256=adapter_result_sha256,
        )
