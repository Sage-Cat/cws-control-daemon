from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable

from cws_control.models import Configuration, NodeEvidence
from cws_control.policy import AqmcParameters, SacmParameters, aqmc_admit, sacm_decide
from cws_control.timing import WRAP_US, TimingValidator


def evidence(source: str, available: bool, utility: float = 1.0, tier: str = "medium", temperature: float = 55.0) -> NodeEvidence:
    return NodeEvidence(
        source_id=source,
        available=available,
        unavailable_reasons=() if available else ("injected-source-loss",),
        frame_count=20 if available else 0,
        expected_frames=20.0,
        rssi_mean=-50.0,
        rssi_std=2.0,
        frequency_spread_hz=2.0,
        csi_mean_amplitude=10.0,
        csi_variance=4.0,
        csi_rms=10.2,
        csi_entropy=2.0,
        confidence=0.9 if available else 0.0,
        reliability=0.9 if available else 0.0,
        timing_valid=available,
        timing_uncertainty_ms=1.0 if available else None,
        timing_drift_ppm=0.0 if available else None,
        timing_points=20 if available else 0,
        trigger=False,
        utility=utility,
        requested_tier=tier,
        motion_score=0.2,
        boot_epoch=1,
        output_drop_ratio=0.0,
        chip_temperature_c=temperature,
    )


def run_failure_scenarios() -> dict[str, Any]:
    cases: list[dict[str, Any]] = []

    def check(name: str, function: Callable[[], tuple[bool, dict[str, Any]]]) -> None:
        try:
            passed, details = function()
            cases.append({"name": name, "passed": bool(passed), "details": details})
        except Exception as exc:
            cases.append({"name": name, "passed": False, "details": {"exception": repr(exc)}})

    def rollover() -> tuple[bool, dict[str, Any]]:
        validator = TimingValidator()
        raws = [WRAP_US - 4000, WRAP_US - 3000, WRAP_US - 2000, WRAP_US - 1000, 0, 1000, 2000, 3000, 4000]
        outputs = [validator.observe(raw, index * 1_000_000, 11) for index, raw in enumerate(raws)]
        events = [item.event for item in outputs if item.event]
        return events.count("timestamp-wrap") == 1 and outputs[-1].boot_epoch == 11 and outputs[-1].valid, {
            "events": events,
            "final": asdict(outputs[-1]),
        }

    def reboot_recovery() -> tuple[bool, dict[str, Any]]:
        validator = TimingValidator()
        for index in range(8):
            validator.observe((index + 1) * 1000, (index + 1) * 1_000_000, 3)
        reset = validator.observe(100, 20_000_000, 4)
        recovered = reset
        for index in range(1, 9):
            recovered = validator.observe(100 + index * 1000, (20 + index) * 1_000_000, 4)
        return reset.event == "reported-boot-epoch-change" and not reset.valid and recovered.valid, {
            "reset": asdict(reset),
            "recovered": asdict(recovered),
        }

    def source_loss() -> tuple[bool, dict[str, Any]]:
        aqmc = AqmcParameters()
        sacm = SacmParameters()
        current = Configuration({"a": "high", "b": "high"})
        nodes = {"a": evidence("a", False, 1.3, "high"), "b": evidence("b", True, 1.1, "medium")}
        candidate = aqmc_admit(nodes, "moderate", aqmc).configuration
        result = sacm_decide(current, candidate, nodes, "moderate", aqmc, sacm)
        return result.action == "accept" and result.reason == "feasibility-override" and result.configuration.active_nodes == ("b",), asdict(result)

    def complete_loss() -> tuple[bool, dict[str, Any]]:
        aqmc = AqmcParameters()
        sacm = SacmParameters()
        nodes = {"a": evidence("a", False), "b": evidence("b", False)}
        candidate = aqmc_admit(nodes, "high", aqmc).configuration
        result = sacm_decide(Configuration({"a": "high", "b": "high"}), candidate, nodes, "high", aqmc, sacm)
        return result.action == "fallback" and not result.configuration.active_nodes, asdict(result)

    def budget_shock() -> tuple[bool, dict[str, Any]]:
        aqmc = AqmcParameters(rate_profile="hardware-40hz", airtime_budget_percent=0.01, qos_budget_percent=0.005)
        nodes = {"a": evidence("a", True, 1.3, "high"), "b": evidence("b", True, 1.2, "high")}
        result = aqmc_admit(nodes, "high", aqmc)
        return not result.configuration.active_nodes and result.diagnostic_low_tier is not None and not result.diagnostic_low_tier.feasible, asdict(result)

    def rate_mapping() -> tuple[bool, dict[str, Any]]:
        paper = AqmcParameters(rate_profile="paper").rates
        hardware = AqmcParameters(rate_profile="hardware-40hz").rates
        return paper == {"off": 0, "low": 2, "medium": 5, "high": 10} and hardware == {"off": 0, "low": 10, "medium": 20, "high": 40}, {
            "paper": paper,
            "hardware_40hz": hardware,
        }

    check("timestamp-rollover-not-reboot", rollover)
    check("reported-reboot-invalidates-then-recovers", reboot_recovery)
    check("single-node-loss-forces-feasible-replacement", source_loss)
    check("complete-node-loss-enters-no-sensing-fallback", complete_loss)
    check("budget-shock-prevents-over-budget-actuation", budget_shock)
    check("paper-and-hardware-rate-mappings-are-distinct", rate_mapping)
    return {
        "schema_version": 1,
        "suite": "failure-aware-timing-validation-multisource",
        "passed": all(case["passed"] for case in cases),
        "passed_cases": sum(case["passed"] for case in cases),
        "total_cases": len(cases),
        "cases": cases,
    }
