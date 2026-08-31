from __future__ import annotations

import copy
import json
import socket
import subprocess
import unittest
from collections.abc import Mapping
from unittest import mock

from cws_control.adapter_protocol import (
    ADAPTER_CAPABILITIES_V1,
    ADAPTER_REQUEST_V1,
    AdapterCallContext,
    AdapterRunner,
    make_adapter_capabilities,
    make_adapter_request,
    opaque_sha256,
    validate_adapter_request,
    validate_adapter_transaction,
)
from cws_control.contracts import ContractError, canonical_sha256, contract


PRE_STATE = {"backend_state": {"vendor_token": "before", "opaque_value": 7}}
DESIRED_STATE = {"backend_state": {"vendor_token": "after", "opaque_value": 9}}
WRONG_STATE = {"backend_state": {"vendor_token": "wrong", "opaque_value": 11}}
PARAMETERS = {"backend_parameters": {"vendor_blob": "synthetic-only"}}


class SyntheticAdapter:
    """In-memory test double; it defines its own opaque state transition."""

    def __init__(
        self,
        *,
        state: Mapping = PRE_STATE,
        apply_state: Mapping = DESIRED_STATE,
        apply_supported: bool = True,
        max_timeout_ms: int = 500,
        read_timeout: bool = False,
        rollback_restores: bool = True,
        apply_raises: bool = False,
    ):
        self.state = copy.deepcopy(dict(state))
        self.apply_state = copy.deepcopy(dict(apply_state))
        self.read_timeout = read_timeout
        self.rollback_restores = rollback_restores
        self.apply_raises = apply_raises
        self.calls: list[tuple[str, str, AdapterCallContext]] = []
        self._capabilities = make_adapter_capabilities(
            adapter_id="synthetic-adapter",
            backend_kind="openwrt",
            max_timeout_ms=max_timeout_ms,
            operations=[
                {
                    "operation_id": "vendor.measurement.rate",
                    "action_boundary": "measurement-control",
                    "apply_supported": apply_supported,
                },
                {
                    "operation_id": "vendor.traffic.path",
                    "action_boundary": "traffic-topology",
                    "apply_supported": False,
                },
            ],
        )

    def capabilities(self) -> Mapping:
        return self._capabilities

    def read_state(
        self,
        operation_id: str,
        parameters: Mapping,
        context: AdapterCallContext,
    ) -> Mapping:
        self.calls.append(("read", operation_id, context))
        if self.read_timeout:
            raise TimeoutError("synthetic timeout")
        return copy.deepcopy(self.state)

    def preview(
        self,
        operation_id: str,
        parameters: Mapping,
        context: AdapterCallContext,
    ) -> Mapping:
        self.calls.append(("preview", operation_id, context))
        return {"backend_preview": "synthetic-plan"}

    def apply(
        self,
        operation_id: str,
        parameters: Mapping,
        context: AdapterCallContext,
    ) -> Mapping:
        self.calls.append(("apply", operation_id, context))
        self.state = copy.deepcopy(self.apply_state)
        if self.apply_raises:
            raise RuntimeError("synthetic apply failure")
        return {"backend_result": "synthetic-apply"}

    def rollback(
        self,
        operation_id: str,
        captured_state: Mapping,
        context: AdapterCallContext,
    ) -> Mapping:
        self.calls.append(("rollback", operation_id, context))
        if self.rollback_restores:
            self.state = copy.deepcopy(dict(captured_state))
        return {"backend_result": "synthetic-rollback"}


def request(
    *,
    mode: str = "observe",
    operation_id: str = "vendor.measurement.rate",
    action_boundary: str = "measurement-control",
    timeout_ms: int = 100,
) -> dict:
    return make_adapter_request(
        request_id="request-a",
        adapter_id="synthetic-adapter",
        operation_id=operation_id,
        action_boundary=action_boundary,
        parameters=PARAMETERS,
        timeout_ms=timeout_ms,
        mode=mode,
        expected_pre_state_sha256=None if mode == "observe" else opaque_sha256(PRE_STATE),
        expected_post_state_sha256=None if mode == "observe" else opaque_sha256(DESIRED_STATE),
    )


class AdapterContractTests(unittest.TestCase):
    def test_contracts_reject_unknown_fields_versions_modes_and_hashes(self) -> None:
        value = request()
        extra = {**value, "portable_operation": "invented"}
        extra["sha256"] = canonical_sha256(extra)
        with self.assertRaisesRegex(ContractError, "unknown-request-field"):
            validate_adapter_request(extra)

        unknown = {**value, "schema_version": "cws-adapter-request/2"}
        unknown["sha256"] = canonical_sha256(unknown)
        with self.assertRaisesRegex(ContractError, "unknown-major-version"):
            validate_adapter_request(unknown)

        invalid_mode = contract(
            ADAPTER_REQUEST_V1,
            {
                key: item
                for key, item in value.items()
                if key not in {"schema_version", "sha256", "mode"}
            }
            | {"mode": "execute"},
        )
        with self.assertRaisesRegex(ContractError, "unknown-request-mode"):
            validate_adapter_request(invalid_mode)

        broken_hash = {**value, "parameters_sha256": "0" * 64}
        broken_hash["sha256"] = canonical_sha256(broken_hash)
        with self.assertRaisesRegex(ContractError, "parameters-hash-mismatch"):
            validate_adapter_request(broken_hash)

    def test_capability_is_versioned_and_backend_operations_stay_opaque(self) -> None:
        capabilities = SyntheticAdapter().capabilities()
        self.assertEqual(capabilities["schema_version"], ADAPTER_CAPABILITIES_V1)
        self.assertEqual(capabilities["backend_kind"], "openwrt")
        self.assertEqual(
            [item["operation_id"] for item in capabilities["operations"]],
            ["vendor.measurement.rate", "vendor.traffic.path"],
        )

    def test_transaction_validator_rejects_unverified_applied_claim(self) -> None:
        adapter = SyntheticAdapter()
        observed = AdapterRunner(
            adapter,
            allowed_operation_ids={"vendor.measurement.rate"},
            monotonic_ns=lambda: 1,
        ).run(request())
        false_claim = {
            **observed,
            "mode": "apply",
            "status": "applied",
            "reason": "apply-verified",
        }
        false_claim["sha256"] = canonical_sha256(false_claim)
        with self.assertRaisesRegex(ContractError, "invalid-applied-transaction"):
            validate_adapter_transaction(false_claim)


class AdapterRunnerTests(unittest.TestCase):
    def runner(self, adapter: SyntheticAdapter, *, allow_apply: bool = False) -> AdapterRunner:
        return AdapterRunner(
            adapter,
            allowed_operation_ids={"vendor.measurement.rate", "vendor.traffic.path"},
            allow_apply=allow_apply,
            monotonic_ns=lambda: 1_000_000,
        )

    def test_default_observe_only_reads_and_returns_hashes(self) -> None:
        adapter = SyntheticAdapter()
        with mock.patch.object(subprocess, "run") as process, mock.patch.object(socket, "socket") as network:
            result = self.runner(adapter).run(request())
        process.assert_not_called()
        network.assert_not_called()
        self.assertEqual(result["status"], "observed")
        self.assertEqual(result["mode"], "observe")
        self.assertEqual(result["pre_state_sha256"], opaque_sha256(PRE_STATE))
        self.assertEqual(result["post_state_sha256"], opaque_sha256(PRE_STATE))
        self.assertEqual([call[0] for call in adapter.calls], ["read"])
        self.assertEqual(adapter.state, PRE_STATE)
        self.assertNotIn("vendor_token", json.dumps(result))
        validate_adapter_transaction(result)

    def test_dry_run_previews_without_mutation(self) -> None:
        adapter = SyntheticAdapter()
        result = self.runner(adapter).run(request(mode="dry-run"))
        self.assertEqual(result["status"], "dry-run")
        self.assertEqual([call[0] for call in adapter.calls], ["read", "preview"])
        self.assertEqual(adapter.state, PRE_STATE)
        self.assertIsNotNone(result["adapter_result_sha256"])
        for _phase, _operation, context in adapter.calls:
            self.assertEqual(context.timeout_ms, 100)
            self.assertEqual(context.deadline_monotonic_ns, 101_000_000)

    def test_apply_requires_capability_and_explicit_latch(self) -> None:
        adapter = SyntheticAdapter()
        rejected = self.runner(adapter).run(request(mode="apply"))
        self.assertEqual((rejected["status"], rejected["reason"]), ("rejected", "apply-latch-required"))
        self.assertEqual(adapter.calls, [])

        unsupported = SyntheticAdapter(apply_supported=False)
        rejected = self.runner(unsupported, allow_apply=True).run(request(mode="apply"))
        self.assertEqual((rejected["status"], rejected["reason"]), ("rejected", "apply-unsupported"))
        self.assertEqual(unsupported.calls, [])

    def test_allowlist_boundary_timeout_and_precondition_fail_closed(self) -> None:
        adapter = SyntheticAdapter()
        denied_runner = AdapterRunner(
            adapter,
            allowed_operation_ids={"vendor.traffic.path"},
            monotonic_ns=lambda: 1,
        )
        denied = denied_runner.run(request())
        self.assertEqual(denied["reason"], "operation-not-allowed")
        self.assertEqual(adapter.calls, [])

        mismatch = self.runner(adapter).run(
            request(operation_id="vendor.traffic.path", action_boundary="measurement-control")
        )
        self.assertEqual(mismatch["reason"], "boundary-mismatch")
        self.assertEqual(adapter.calls, [])

        timeout_adapter = SyntheticAdapter(max_timeout_ms=50)
        timeout = self.runner(timeout_adapter).run(request(timeout_ms=100))
        self.assertEqual(timeout["reason"], "timeout-exceeds-capability")
        self.assertEqual(timeout_adapter.calls, [])

        stale_adapter = SyntheticAdapter()
        stale_request = request(mode="apply")
        stale_request["expected_pre_state_sha256"] = "f" * 64
        stale_request["sha256"] = canonical_sha256(stale_request)
        stale = self.runner(stale_adapter, allow_apply=True).run(stale_request)
        self.assertEqual(stale["reason"], "stale-precondition")
        self.assertEqual([call[0] for call in stale_adapter.calls], ["read"])
        self.assertEqual(stale_adapter.state, PRE_STATE)

        read_timeout = SyntheticAdapter(read_timeout=True)
        failed = self.runner(read_timeout).run(request())
        self.assertEqual((failed["status"], failed["reason"]), ("failed", "adapter-timeout"))

        ticks = iter((1, 100_000_002))
        deadline_adapter = SyntheticAdapter()
        deadline_runner = AdapterRunner(
            deadline_adapter,
            allowed_operation_ids={"vendor.measurement.rate"},
            monotonic_ns=lambda: next(ticks),
        )
        expired = deadline_runner.run(request())
        self.assertEqual(
            (expired["status"], expired["reason"]),
            ("failed", "adapter-deadline-exceeded"),
        )

    def test_verified_apply_requires_exact_postcondition(self) -> None:
        adapter = SyntheticAdapter()
        with mock.patch.object(subprocess, "run") as process, mock.patch.object(socket, "socket") as network:
            result = self.runner(adapter, allow_apply=True).run(request(mode="apply"))
        process.assert_not_called()
        network.assert_not_called()
        self.assertEqual((result["status"], result["reason"]), ("applied", "apply-verified"))
        self.assertEqual(result["post_state_sha256"], opaque_sha256(DESIRED_STATE))
        self.assertEqual([call[0] for call in adapter.calls], ["read", "apply", "read"])
        self.assertEqual(adapter.state, DESIRED_STATE)

    def test_postcondition_failure_rolls_back_to_captured_state(self) -> None:
        adapter = SyntheticAdapter(apply_state=WRONG_STATE)
        result = self.runner(adapter, allow_apply=True).run(request(mode="apply"))
        self.assertEqual((result["status"], result["reason"]), ("rolled-back", "postcondition-mismatch"))
        self.assertEqual(result["rollback_state_sha256"], opaque_sha256(PRE_STATE))
        self.assertEqual(adapter.state, PRE_STATE)
        self.assertEqual([call[0] for call in adapter.calls], ["read", "apply", "read", "rollback", "read"])

    def test_apply_exception_still_rolls_back_and_reads_back(self) -> None:
        adapter = SyntheticAdapter(apply_state=WRONG_STATE, apply_raises=True)
        result = self.runner(adapter, allow_apply=True).run(request(mode="apply"))
        self.assertEqual((result["status"], result["reason"]), ("rolled-back", "adapter-error"))
        self.assertEqual(result["rollback_state_sha256"], opaque_sha256(PRE_STATE))
        self.assertEqual(adapter.state, PRE_STATE)
        self.assertEqual([call[0] for call in adapter.calls], ["read", "apply", "rollback", "read"])

    def test_rollback_failure_is_explicit(self) -> None:
        adapter = SyntheticAdapter(apply_state=WRONG_STATE, rollback_restores=False)
        result = self.runner(adapter, allow_apply=True).run(request(mode="apply"))
        self.assertEqual((result["status"], result["reason"]), ("rollback-failed", "postcondition-mismatch"))
        self.assertEqual(result["rollback_state_sha256"], opaque_sha256(WRONG_STATE))
        self.assertEqual(adapter.state, WRONG_STATE)


if __name__ == "__main__":
    unittest.main()
