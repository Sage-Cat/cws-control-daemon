from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from typing import Mapping

from cws_control.models import (
    AqmcResult,
    BudgetResult,
    Configuration,
    NodeEvidence,
    SacmResult,
    clamp,
)


LOAD_WEIGHTS = {"light": 0.20, "moderate": 0.35, "elevated": 0.55, "high": 0.85}
RATE_PROFILES = {
    "paper": {"off": 0, "low": 2, "medium": 5, "high": 10},
    "hardware-40hz": {"off": 0, "low": 10, "medium": 20, "high": 40},
}


@dataclass(frozen=True)
class AqmcParameters:
    rate_profile: str = "paper"
    bytes_per_sample: int = 512
    phy_rate_mbps: float = 54.0
    airtime_budget_percent: float = 10.0
    qos_budget_percent: float = 5.0
    probe_rate_hz: int = 0

    def __post_init__(self) -> None:
        if self.rate_profile not in RATE_PROFILES:
            raise ValueError(f"unknown rate profile: {self.rate_profile}")
        if self.bytes_per_sample <= 0 or self.phy_rate_mbps <= 0:
            raise ValueError("AQ-MC sample size and PHY rate must be positive")
        if self.airtime_budget_percent <= 0 or self.qos_budget_percent <= 0:
            raise ValueError("AQ-MC budgets must be positive")
        if not 0 <= self.probe_rate_hz <= 50:
            raise ValueError("AQ-MC probe rate must be from 0 to 50 Hz")

    @property
    def rates(self) -> dict[str, int]:
        return RATE_PROFILES[self.rate_profile]


def aqmc_utility(rssi_variation_scaled: float, frequency_spread_hz: float, confidence: float) -> float:
    return 0.45 * rssi_variation_scaled + 0.35 * (frequency_spread_hz / 5.0) + 0.20 * confidence


def tier_for_utility(utility: float, trigger: bool) -> str:
    if utility >= 1.2 or trigger:
        return "high"
    if utility >= 0.75:
        return "medium"
    return "low"


def budget(configuration: Configuration, load: str, parameters: AqmcParameters) -> BudgetResult:
    if load not in LOAD_WEIGHTS:
        raise ValueError(f"unknown load category: {load}")
    total_hz = sum(
        parameters.rates[configuration.tier(node)]
        if configuration.tier(node) != "off"
        else parameters.probe_rate_hz
        for node in configuration.tiers
    )
    airtime = 100.0 * parameters.bytes_per_sample * 8.0 * total_hz / (parameters.phy_rate_mbps * 1e6)
    penalty = LOAD_WEIGHTS[load] * airtime
    if airtime > parameters.airtime_budget_percent:
        return BudgetResult(False, "airtime-budget", airtime, penalty)
    if penalty > parameters.qos_budget_percent:
        return BudgetResult(False, "qos-budget", airtime, penalty)
    return BudgetResult(True, "feasible", airtime, penalty)


def aqmc_admit(
    evidence: Mapping[str, NodeEvidence],
    load: str,
    parameters: AqmcParameters,
) -> AqmcResult:
    utilities = {source: item.utility for source, item in evidence.items()}
    requested = {source: item.requested_tier for source, item in evidence.items()}
    configuration = Configuration({source: "off" for source in evidence})
    rejected: dict[str, str] = {}
    for source, item in sorted(evidence.items(), key=lambda pair: (-pair[1].utility, pair[0])):
        if not item.available:
            rejected[source] = "unavailable"
            continue
        tentative = Configuration({**configuration.tiers, source: item.requested_tier})
        result = budget(tentative, load, parameters)
        if result.feasible:
            configuration = tentative
        else:
            rejected[source] = result.reason
    accepted_budget = budget(configuration, load, parameters)
    diagnostic = None
    if not configuration.active_nodes:
        available = [item for item in evidence.values() if item.available]
        if available:
            best = max(available, key=lambda item: (item.utility, item.source_id))
            diagnostic = budget(
                Configuration({source: ("low" if source == best.source_id else "off") for source in evidence}),
                load,
                parameters,
            )
    return AqmcResult(configuration, accepted_budget, utilities, requested, rejected, diagnostic)


@dataclass(frozen=True)
class SacmParameters:
    quality_weight: float = 0.70
    airtime_weight: float = 0.15
    qos_weight: float = 0.15
    alpha_node: float = 0.010
    alpha_rate: float = 0.010
    alpha_bytes: float = 0.0
    epsilon: float = 0.0

    def __post_init__(self) -> None:
        if abs(self.quality_weight + self.airtime_weight + self.qos_weight - 1.0) > 1e-9:
            raise ValueError("SACM utility weights must sum to 1")


def configuration_feasible(
    configuration: Configuration,
    evidence: Mapping[str, NodeEvidence],
    load: str,
    aqmc: AqmcParameters,
) -> bool:
    if not configuration.active_nodes:
        return False
    if any(not evidence.get(node) or not evidence[node].available for node in configuration.active_nodes):
        return False
    return budget(configuration, load, aqmc).feasible


def normalized_quality(configuration: Configuration, evidence: Mapping[str, NodeEvidence]) -> float:
    active = [evidence[node].utility for node in configuration.active_nodes if node in evidence]
    return clamp(fmean(active) / 1.2) if active else 0.0


def configuration_utility(
    configuration: Configuration,
    evidence: Mapping[str, NodeEvidence],
    load: str,
    aqmc: AqmcParameters,
    sacm: SacmParameters,
) -> float:
    result = budget(configuration, load, aqmc)
    quality = normalized_quality(configuration, evidence)
    normalized_airtime = clamp(result.airtime_percent / aqmc.airtime_budget_percent)
    normalized_qos = clamp(result.qos_penalty_percent / aqmc.qos_budget_percent)
    return sacm.quality_weight * quality - sacm.airtime_weight * normalized_airtime - sacm.qos_weight * normalized_qos


def transition_metrics(previous: Configuration, candidate: Configuration, node_count: int) -> tuple[int, int, int]:
    nodes = sorted(set(previous.tiers) | set(candidate.tiers))
    hamming = sum((previous.tier(node) == "off") != (candidate.tier(node) == "off") for node in nodes)
    rate = sum(
        previous.tier(node) != candidate.tier(node)
        and previous.tier(node) != "off"
        and candidate.tier(node) != "off"
        for node in nodes
    )
    changed = previous.tiers != candidate.tiers
    estimated_bytes = (128 if changed else 0) + 64 * hamming + 32 * rate
    return hamming, rate, estimated_bytes


def sacm_decide(
    previous: Configuration | None,
    candidate: Configuration,
    evidence: Mapping[str, NodeEvidence],
    load: str,
    aqmc: AqmcParameters,
    parameters: SacmParameters,
) -> SacmResult:
    candidate_feasible = configuration_feasible(candidate, evidence, load, aqmc)
    incumbent = previous or Configuration({source: "off" for source in evidence})
    incumbent_feasible = previous is not None and configuration_feasible(incumbent, evidence, load, aqmc)
    candidate_utility = configuration_utility(candidate, evidence, load, aqmc, parameters)
    incumbent_utility = configuration_utility(incumbent, evidence, load, aqmc, parameters)
    gain = candidate_utility - incumbent_utility
    hamming, rate, estimated_bytes = transition_metrics(incumbent, candidate, max(1, len(evidence)))
    bmax = max(1, 128 + 64 * max(1, len(evidence)) + 32 * max(1, len(evidence)))
    cost = (
        parameters.alpha_node * hamming / max(1, len(evidence))
        + parameters.alpha_rate * rate / max(1, len(evidence))
        + parameters.alpha_bytes * estimated_bytes / bmax
    )
    if previous is not None and candidate == incumbent and incumbent_feasible:
        return SacmResult(
            incumbent,
            "hold",
            "unchanged",
            candidate_feasible,
            True,
            candidate_utility,
            incumbent_utility,
            gain,
            0.0,
            0,
        )
    if candidate_feasible and (not incumbent_feasible or gain >= cost + parameters.epsilon):
        reason = "initialize" if previous is None else ("feasibility-override" if not incumbent_feasible else "gain-covers-transition")
        return SacmResult(candidate, "accept", reason, True, incumbent_feasible, candidate_utility, incumbent_utility, gain, cost, estimated_bytes)
    if incumbent_feasible:
        reason = "candidate-infeasible" if not candidate_feasible else "gain-below-transition"
        return SacmResult(incumbent, "hold", reason, candidate_feasible, True, candidate_utility, incumbent_utility, gain, cost, 0)
    fallback = Configuration({source: "off" for source in evidence})
    return SacmResult(fallback, "fallback", "no-feasible-active-sensing", candidate_feasible, False, candidate_utility, incumbent_utility, gain, cost, 0)


def aqmc_direct_decide(
    previous: Configuration | None,
    candidate: Configuration,
    evidence: Mapping[str, NodeEvidence],
    load: str,
    aqmc: AqmcParameters,
    utility_parameters: SacmParameters,
) -> SacmResult:
    """Expose AQ-MC directly for the no-maintenance ablation."""
    incumbent = previous or Configuration({source: "off" for source in evidence})
    candidate_feasible = configuration_feasible(candidate, evidence, load, aqmc)
    incumbent_feasible = previous is not None and configuration_feasible(incumbent, evidence, load, aqmc)
    candidate_utility = configuration_utility(candidate, evidence, load, aqmc, utility_parameters)
    incumbent_utility = configuration_utility(incumbent, evidence, load, aqmc, utility_parameters)
    _hamming, _rate, estimated_bytes = transition_metrics(incumbent, candidate, max(1, len(evidence)))
    changed = candidate != incumbent
    return SacmResult(
        candidate,
        "accept" if changed else "hold",
        "aqmc-direct" if changed else "unchanged",
        candidate_feasible,
        incumbent_feasible,
        candidate_utility,
        incumbent_utility,
        candidate_utility - incumbent_utility,
        0.0,
        estimated_bytes if changed else 0,
    )
