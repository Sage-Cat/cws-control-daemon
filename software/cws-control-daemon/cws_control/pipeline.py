"""Pure M1--M4 replay/reference path.

The module intentionally takes already collected records, qualified descriptors
and precomputed candidate metrics.  It opens no files and never performs
network, hardware, or actuation operations.
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from copy import deepcopy
from statistics import fmean, pstdev
from typing import Any, Mapping, Sequence

from cws_control.contracts import (
    ContractError, EVIDENCE_BUNDLE_V1, M1_VERDICT_V1, M2_FUSION_RESULT_V1,
    M3_CANDIDATE_RESULT_V1, M4_DECISION_V1, canonical_sha256, contract,
    require_hash, require_id, require_reason, source_record, verify_contract,
)


M2_OPERATOR_VERSION = "m2-reference-fusion/1"
M2_RECOVERED_ANALYZER_SHA256 = "a1c002c3406d1350645c19538192fc8a68f969c90fc40ec4243d9434cd263715"
BM_CSF_OPERATOR_VERSION = "bm-csf-reference/1"
QISS_OPERATOR_VERSION = "qiss-classical-supportive-search/1"
SACM_COMPONENT_VERSION = "sacm-replay-reference/1"
DESCRIPTOR_DIMENSIONS = 12


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _rejected(version: str, result_id: str, reason: str, **extra: Any) -> dict[str, Any]:
    require_id(result_id, "result-id")
    require_reason(reason)
    return contract(version, {"result_id": result_id, "admitted": False, "reasons": [reason], **extra})


def m1_admit(*, bundle_id: str, interval_start_ns: int, interval_end_ns: int,
             expected_sources: Sequence[str], records: Sequence[Mapping[str, Any]],
             producer_versions: Mapping[str, str] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Freeze source facts into one immutable evidence bundle and type its gate."""
    require_id(bundle_id, "bundle-id")
    if not isinstance(interval_start_ns, int) or not isinstance(interval_end_ns, int) or interval_start_ns >= interval_end_ns:
        raise ContractError("invalid-interval")
    expected = tuple(sorted({require_id(item, "source-id") for item in expected_sources}))
    normalized = [source_record(item) for item in records]
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in normalized:
        by_source[item["source_id"]].append(item)
    reasons: set[str] = set()
    observed = tuple(sorted(by_source))
    for source in set(observed) - set(expected):
        reasons.add("unexpected-source")
    facts: dict[str, dict[str, Any]] = {}
    for source in expected:
        rows = by_source.get(source, [])
        if not rows:
            reasons.add("missing-source")
            facts[source] = {"gap": None, "duplicate": None, "reset": None, "reconnect": None,
                             "timing_valid": None, "pair_covered": None, "provenance_valid": None,
                             "stale": None}
            continue
        merged: dict[str, Any] = {}
        for key in ("gap", "duplicate", "reset", "reconnect", "timing_valid", "pair_covered", "provenance_valid", "stale"):
            values = [row.get("facts", {}).get(key) for row in rows]
            if key in {"timing_valid", "pair_covered", "provenance_valid"}:
                merged[key] = all(value is True for value in values)
            else:
                merged[key] = any(value is True or (isinstance(value, int) and not isinstance(value, bool) and value > 0) for value in values)
        facts[source] = merged
        if merged["gap"] is True: reasons.add("source-gap")
        if merged["duplicate"] is True: reasons.add("duplicate-record")
        if merged["reset"] is True: reasons.add("source-reset")
        if merged["reconnect"] is True: reasons.add("source-reconnect")
        if merged["timing_valid"] is not True: reasons.add("timing-unqualified")
        if merged["pair_covered"] is not True: reasons.add("pair-uncovered")
        if merged["provenance_valid"] is not True: reasons.add("provenance-unverified")
        if merged["stale"] is True: reasons.add("source-stale")
    hashes = sorted(row["sha256"] for row in normalized)
    bundle = contract(EVIDENCE_BUNDLE_V1, {
        "bundle_id": bundle_id, "interval": {"start_ns": interval_start_ns, "end_ns": interval_end_ns},
        "record_hashes": hashes, "expected_sources": list(expected), "observed_sources": list(observed),
        "source_facts": facts, "producer_versions": dict(sorted((producer_versions or {}).items())),
    })
    verdict = contract(M1_VERDICT_V1, {
        "result_id": bundle_id + "-m1", "bundle_sha256": bundle["sha256"],
        "admitted": not reasons, "reasons": sorted(reasons), "source_facts": facts,
    })
    return bundle, verdict


def _artifact(artifact: Mapping[str, Any]) -> dict[str, Any]:
    model = verify_contract(artifact, versions={"cws-m2-model-artifact/1"}, required=("operator_version", "scaler_mean", "scaler_scale", "class_labels", "coefficients", "intercepts", "provenance"))
    if model["operator_version"] != M2_OPERATOR_VERSION:
        raise ContractError("unknown-m2-operator")
    mean, scale = model["scaler_mean"], model["scaler_scale"]
    if not isinstance(mean, list) or not isinstance(scale, list) or len(mean) != len(scale) or not mean:
        raise ContractError("invalid-scaler")
    if any(not _finite_number(v) for v in mean + scale) or any(float(v) == 0 for v in scale):
        raise ContractError("invalid-scaler")
    labels, coefficients, intercepts = model["class_labels"], model["coefficients"], model["intercepts"]
    if not isinstance(model["provenance"], Mapping) or not isinstance(labels, list) or len(labels) < 2 or not isinstance(coefficients, list) or not isinstance(intercepts, list):
        raise ContractError("invalid-model")
    try:
        validated_labels = [require_id(label, "class-label") for label in labels]
    except ContractError as exc:
        raise ContractError("invalid-class-label") from exc
    if len(set(validated_labels)) != len(validated_labels):
        raise ContractError("duplicate-class-label")
    width = len(mean)
    if len(labels) == 2 and len(coefficients) == len(intercepts) == 1:
        rows = 1
    elif len(coefficients) == len(intercepts) == len(labels):
        rows = len(labels)
    else:
        raise ContractError("invalid-model-shape")
    if rows != len(coefficients) or any(not isinstance(row, list) or len(row) != width for row in coefficients):
        raise ContractError("invalid-model-shape")
    if any(not _finite_number(value) for row in coefficients for value in row) or any(not _finite_number(value) for value in intercepts):
        raise ContractError("invalid-model-number")
    return model


def _predict(model: Mapping[str, Any], vector: list[float]) -> dict[str, Any]:
    scaled = [(value - float(mean)) / float(scale) for value, mean, scale in zip(vector, model["scaler_mean"], model["scaler_scale"])]
    scores = [sum(weight * value for weight, value in zip(row, scaled)) + float(intercept)
              for row, intercept in zip(model["coefficients"], model["intercepts"])]
    if any(not math.isfinite(value) for value in scaled + scores):
        raise ContractError("nonfinite-prediction")
    labels = model["class_labels"]
    if len(labels) == 2 and len(scores) == 1:
        probability = 1.0 / (1.0 + math.exp(-max(-709.0, min(709.0, scores[0]))))
        index = 1 if scores[0] >= 0.0 else 0
        return {"label": labels[index], "scores": scores, "positive_probability": probability}
    index = max(range(len(scores)), key=lambda item: (scores[item], -item))
    return {"label": labels[index], "scores": scores}


def m2_fuse(*, result_id: str, bundle: Mapping[str, Any], m1_verdict: Mapping[str, Any],
            windows: Sequence[Mapping[str, Any]], model_artifact: Mapping[str, Any],
            paired_path_qualification: Mapping[str, Any], extractor_provenance: Mapping[str, Any]) -> dict[str, Any]:
    """Execute the new reference operator, not a historical run reproduction."""
    try:
        bundle = verify_contract(bundle, versions={EVIDENCE_BUNDLE_V1}, required=("bundle_id", "interval", "record_hashes"))
        verdict = verify_contract(m1_verdict, versions={M1_VERDICT_V1}, required=("bundle_sha256", "admitted", "reasons"))
        if verdict["bundle_sha256"] != bundle["sha256"]: raise ContractError("bundle-correlation-mismatch")
        if verdict["admitted"] is not True: raise ContractError("m1-rejected")
        if not isinstance(paired_path_qualification, Mapping) or paired_path_qualification.get("qualified") is not True:
            raise ContractError("paired-path-unqualified")
        require_id(paired_path_qualification.get("protocol_id"), "paired-path-protocol-id")
        if not isinstance(extractor_provenance, Mapping) or not isinstance(extractor_provenance.get("extractor_version"), str):
            raise ContractError("missing-extractor-provenance")
        if extractor_provenance.get("operator_reference_sha256") != M2_RECOVERED_ANALYZER_SHA256:
            raise ContractError("m2-operator-provenance-mismatch")
        model = _artifact(model_artifact)
        groups: dict[tuple[str, str], list[list[float]]] = defaultdict(list)
        input_hashes: list[str] = []
        for item in windows:
            if not isinstance(item, Mapping) or item.get("qualified") is not True:
                raise ContractError("descriptor-unqualified")
            node, block = require_id(item.get("node_id"), "node-id"), require_id(item.get("block_id"), "block-id")
            values = item.get("values")
            if not isinstance(values, list) or len(values) != DESCRIPTOR_DIMENSIONS or any(not _finite_number(v) for v in values):
                raise ContractError("invalid-12-field-descriptor")
            digest = item.get("sha256") or canonical_sha256(dict(item))
            require_hash(digest, "descriptor-hash")
            if item.get("sha256") is not None and digest != canonical_sha256(dict(item)):
                raise ContractError("hash-mismatch")
            groups[(block, node)].append([float(value) for value in values]); input_hashes.append(digest)
        if not groups: raise ContractError("missing-descriptors")
        blocks: dict[str, dict[str, list[float]]] = defaultdict(dict)
        for (block, node), rows in sorted(groups.items()):
            aggregate: list[float] = []
            for index in range(DESCRIPTOR_DIMENSIONS):
                column = [row[index] for row in rows]
                aggregate.extend((fmean(column), pstdev(column), min(column), max(column)))
            blocks[block][node] = aggregate
        node_order = tuple(sorted({node for _, node in groups}))
        results = []
        for block in sorted(blocks):
            if tuple(sorted(blocks[block])) != node_order: raise ContractError("block-node-set-mismatch")
            vector = [value for node in node_order for value in blocks[block][node]]
            if len(vector) != len(model["scaler_mean"]): raise ContractError("model-feature-width-mismatch")
            results.append({"block_id": block, "prediction": _predict(model, vector), "vector": vector})
        return contract(M2_FUSION_RESULT_V1, {
            "result_id": result_id, "admitted": True, "reasons": [], "bundle_sha256": bundle["sha256"],
            "m1_verdict_sha256": verdict["sha256"], "model_sha256": model["sha256"],
            "operator_version": M2_OPERATOR_VERSION, "operator_scope": "reference-operator-not-historical-execution-reproduction",
            "paired_path_qualification": dict(paired_path_qualification), "extractor_provenance": dict(extractor_provenance),
            "model_provenance": dict(model["provenance"]), "operator_reference_sha256": M2_RECOVERED_ANALYZER_SHA256,
            "node_order": list(node_order), "input_descriptor_hashes": sorted(input_hashes), "blocks": results,
            "ledger": {"computational_eligibility": "qualified-synthetic-or-replay-input", "scientific_result_eligibility": "not-established-by-reference-pipeline", "resource": {"windows": len(windows), "blocks": len(results)}, "quality": "not-inferred"},
        })
    except (ContractError, TypeError, ValueError, OverflowError) as exc:
        reason = str(exc) if isinstance(exc, ContractError) else "invalid-m2-input"
        return _rejected(M2_FUSION_RESULT_V1, result_id, reason, operator_version=M2_OPERATOR_VERSION)


def _candidate_gates(candidate: Mapping[str, Any], ref: Mapping[str, Any], *, anchor: str,
                     evidence_hash: str, model_hash: str, quality_protocol_id: str,
                     record_budget: int | None, node_budget: int | None,
                     service_budget: float | None, service_required: bool) -> list[str]:
    reasons: list[str] = []
    node_ids = candidate.get("node_ids")
    if not isinstance(node_ids, list) or anchor not in node_ids: reasons.append("missing-anchor")
    for field, expected in (("evidence_bundle_sha256", evidence_hash), ("model_sha256", model_hash), ("quality_protocol_id", quality_protocol_id)):
        if candidate.get(field) != expected: reasons.append("protocol-mismatch")
    if candidate.get("qc_passed") is not True: reasons.append("qc-failed")
    for field, reason in (("quality_binary", "binary-quality-margin-failed"), ("quality_three_state", "three-state-quality-margin-failed")):
        quality, reference_quality = candidate.get(field), ref.get(field)
        if not isinstance(quality, (int, float)) or not isinstance(reference_quality, (int, float)) or quality < float(reference_quality) - 0.15:
            reasons.append(reason)
    records, nodes = candidate.get("raw_rows"), candidate.get("node_count")
    if not isinstance(records, int) or records < 0: reasons.append("invalid-raw-row-count")
    elif record_budget is not None and records > record_budget: reasons.append("record-budget-exceeded")
    if not isinstance(nodes, int) or nodes < 1: reasons.append("invalid-node-count")
    elif node_budget is not None and nodes > node_budget: reasons.append("node-budget-exceeded")
    service = candidate.get("service_impact")
    if service_required and not isinstance(service, (int, float)): reasons.append("service-evidence-missing")
    elif service_budget is not None and (not isinstance(service, (int, float)) or service > service_budget): reasons.append("service-budget-exceeded")
    return sorted(set(reasons))


def m3_select(*, result_id: str, candidates: Sequence[Mapping[str, Any]], candidate_universe_ids: Sequence[str],
              anchor_id: str, full_reference_id: str, evidence_bundle_sha256: str, model_sha256: str,
              quality_protocol_id: str, record_budget: int | None = None,
              node_budget: int | None = None, service_budget: float | None = None,
              service_required: bool = False, solver: str = "exhaustive", seed: int = 0,
              search_budget: int | None = None) -> dict[str, Any]:
    """BM-CSF exhaustive reference; QISS only chooses a seeded evaluation order."""
    try:
        anchor_id, full_reference_id = require_id(anchor_id, "anchor-id"), require_id(full_reference_id, "full-reference-id")
        require_hash(evidence_bundle_sha256, "evidence-bundle-sha256"); require_hash(model_sha256, "model-sha256")
        require_id(quality_protocol_id, "quality-protocol-id")
        if solver not in {"exhaustive", "qiss"}: raise ContractError("unknown-solver")
        universe_values = [require_id(value, "candidate-id") for value in candidate_universe_ids]
        if len(set(universe_values)) != len(universe_values): raise ContractError("duplicate-candidate-universe-id")
        universe = sorted(universe_values)
        raw_candidates = [deepcopy(dict(item)) for item in candidates]
        candidate_ids = [require_id(item.get("candidate_id"), "candidate-id") for item in raw_candidates]
        if len(set(candidate_ids)) != len(candidate_ids): raise ContractError("duplicate-candidate-id")
        indexed = {}
        for item in raw_candidates:
            supplied = item.get("sha256")
            digest = supplied or canonical_sha256(item)
            require_hash(digest, "candidate-sha256")
            if supplied is not None and digest != canonical_sha256(item): raise ContractError("hash-mismatch")
            item["sha256"] = digest
            indexed[item["candidate_id"]] = item
        if sorted(indexed) != universe or full_reference_id not in indexed: raise ContractError("incomplete-candidate-universe")
        reference = indexed[full_reference_id]
        if not isinstance(reference.get("quality_binary"), (int, float)) or not isinstance(reference.get("quality_three_state"), (int, float)):
            raise ContractError("reference-quality-missing")
        rows = []
        for candidate_id in universe:
            candidate = indexed[candidate_id]
            gates = _candidate_gates(candidate, reference, anchor=anchor_id, evidence_hash=evidence_bundle_sha256,
                                     model_hash=model_sha256, quality_protocol_id=quality_protocol_id,
                                     record_budget=record_budget, node_budget=node_budget, service_budget=service_budget,
                                     service_required=service_required)
            rows.append({"candidate_id": candidate_id, "admitted": not gates, "reasons": gates,
                         "candidate_sha256": candidate["sha256"], "raw_rows": candidate.get("raw_rows"), "node_count": candidate.get("node_count"),
                         "service_impact": candidate.get("service_impact")})
        reference_row = next(row for row in rows if row["candidate_id"] == full_reference_id)
        ordered = sorted(rows, key=lambda row: (row["raw_rows"] if isinstance(row["raw_rows"], int) else 10**18,
                                                 row["node_count"] if isinstance(row["node_count"], int) else 10**18, row["candidate_id"]))
        visited: list[str]
        qiss_state: dict[str, Any] | None = None
        if solver == "qiss":
            budget = len(ordered) if search_budget is None else max(0, search_budget)
            rng = random.Random(seed)
            # Classical p/pi sampling without replacement across the declared universe.
            all_nodes = sorted({node for item in indexed.values() for node in item.get("node_ids", [])})
            if any(not isinstance(item.get("node_ids"), list) or not isinstance(item.get("tiers", {}), Mapping) for item in indexed.values()):
                raise ContractError("invalid-candidate-qiss-state")
            tiers = sorted({tier for item in indexed.values() for tier in item.get("tiers", {}).values()})
            if any(not isinstance(tier, str) for tier in tiers):
                raise ContractError("invalid-candidate-tier")
            eta = 0.20
            active_probabilities = {node: 0.5 for node in all_nodes}
            categorical = {node: {tier: 1.0 / len(tiers) for tier in tiers} for node in all_nodes}
            pool = list(ordered); visited = []; update_trace: list[dict[str, Any]] = []
            while pool and len(visited) < budget:
                log_likelihoods: list[float] = []
                for row in pool:
                    item = indexed[row["candidate_id"]]
                    likelihood = 0.0
                    for node in all_nodes:
                        probability = active_probabilities[node] if node in item["node_ids"] else 1.0 - active_probabilities[node]
                        likelihood += math.log(max(probability, 1e-300))
                        if node in item["node_ids"] and tiers:
                            tier = item["tiers"].get(node)
                            if tier not in categorical[node]:
                                raise ContractError("invalid-candidate-tier")
                            likelihood += math.log(max(categorical[node][tier], 1e-300))
                    log_likelihoods.append(likelihood)
                maximum = max(log_likelihoods)
                weights = [math.exp(value - maximum) for value in log_likelihoods]
                pick = rng.choices(range(len(pool)), weights=weights, k=1)[0]
                row = pool.pop(pick); visited.append(row["candidate_id"])
                trace: dict[str, Any] = {"candidate_id": row["candidate_id"], "eligible": row["admitted"], "sampling_weight": weights[pick], "active_probabilities_before": dict(active_probabilities)}
                if row["admitted"]:
                    item = indexed[row["candidate_id"]]
                    for node in all_nodes:
                        active_probabilities[node] = (1.0 - eta) * active_probabilities[node] + eta * float(node in item["node_ids"])
                        if node in item["node_ids"]:
                            selected_tier = item["tiers"].get(node)
                            for tier in tiers:
                                categorical[node][tier] = (1.0 - eta) * categorical[node][tier] + eta * float(tier == selected_tier)
                    trace["updated"] = True
                else:
                    trace["updated"] = False
                trace["active_probabilities_after"] = dict(active_probabilities)
                update_trace.append(trace)
            # Gates stay BM-CSF gates; unvisited candidates cannot be invented as accepted.
            accepted_ids = {row["candidate_id"] for row in ordered if row["admitted"]}
            accepted_seen = [row for row in ordered if row["candidate_id"] in visited and row["candidate_id"] in accepted_ids]
            selected_pool = accepted_seen
            qiss_state = {"seed": seed, "search_budget": budget, "visited": visited, "eta": eta,
                          "active_probabilities": active_probabilities, "tier_probabilities": categorical,
                          "update_trace": update_trace,
                          "update_rule": "classical-probability-update-toward-eligible-candidate"}
        else:
            visited = [row["candidate_id"] for row in ordered]
            selected_pool = [row for row in ordered if row["admitted"]]
        reduced = [row for row in selected_pool if row["candidate_id"] != full_reference_id]
        if reduced:
            status, selected, reasons = "selected", reduced[0]["candidate_id"], []
        elif reference_row["admitted"]:
            status, selected, reasons = "reference-only", full_reference_id, ["no-reduced-admissible-candidate"]
        else:
            status, selected, reasons = "infeasible", None, ["reference-gates-failed"]
        return contract(M3_CANDIDATE_RESULT_V1, {
            "result_id": result_id, "admitted": status != "infeasible", "reasons": reasons, "status": status,
            "selected_candidate_id": selected, "anchor_id": anchor_id, "full_reference_id": full_reference_id,
            "selected_candidate_sha256": indexed[selected]["sha256"] if selected is not None else None,
            "candidate_universe_ids": universe, "evidence_bundle_sha256": evidence_bundle_sha256, "model_sha256": model_sha256,
            "quality_protocol_id": quality_protocol_id, "quality_margins": {"binary": 0.15, "three_state": 0.15},
            "budgets": {"records": record_budget, "nodes": node_budget, "service": service_budget, "service_required": service_required},
            "solver": solver, "operator_version": BM_CSF_OPERATOR_VERSION if solver == "exhaustive" else QISS_OPERATOR_VERSION,
            "seed": seed, "search_budget": search_budget, "ordering": [row["candidate_id"] for row in ordered], "visited": visited,
            "candidate_ledger": rows, "qiss_state": qiss_state,
        })
    except (ContractError, TypeError, ValueError, OverflowError) as exc:
        reason = str(exc) if isinstance(exc, ContractError) else "invalid-m3-input"
        return _rejected(M3_CANDIDATE_RESULT_V1, result_id, reason, status="infeasible")


def _normal_utility(inputs: Mapping[str, Any], parameters: Mapping[str, float]) -> float:
    for field in ("quality", "airtime", "service"):
        value = inputs.get(field)
        if not isinstance(value, (int, float)) or not 0 <= value <= 1: raise ContractError("invalid-normalized-utility-input")
    return parameters["quality_weight"] * inputs["quality"] - parameters["airtime_weight"] * inputs["airtime"] - parameters["service_weight"] * inputs["service"]


def default_sacm_parameters() -> dict[str, float]:
    return {"quality_weight": 0.60, "airtime_weight": 0.20, "service_weight": 0.20,
            "alpha_node": 0.65, "alpha_rate": 0.010, "alpha_byte": 0.0, "epsilon": 0.0}


def serialize_sacm_state(state: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(state, Mapping) or "incumbent" not in state or "decision_epoch" not in state:
        raise ContractError("invalid-sacm-state")
    epoch, incumbent = state["decision_epoch"], state["incumbent"]
    if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0 or not isinstance(incumbent, Mapping):
        raise ContractError("invalid-sacm-state")
    if not isinstance(incumbent.get("candidate_id"), str) or not isinstance(incumbent.get("normalized_utility"), Mapping):
        raise ContractError("invalid-sacm-state")
    return contract("cws-sacm-state/1", dict(state))


def restore_sacm_state(value: Mapping[str, Any]) -> dict[str, Any]:
    state = verify_contract(value, versions={"cws-sacm-state/1"}, required=("incumbent", "decision_epoch"))
    return serialize_sacm_state(state)


def m4_decide(*, decision_id: str, m3_result: Mapping[str, Any], candidate: Mapping[str, Any],
              state: Mapping[str, Any] | None = None, parameters: Mapping[str, float] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Stateful SACM transition for exactly one already-tested M3 candidate."""
    preserved_state: dict[str, Any] | None = None
    if state is not None:
        try:
            preserved_state = restore_sacm_state(state)
        except (ContractError, TypeError, ValueError):
            fallback_state = serialize_sacm_state({"component_version": SACM_COMPONENT_VERSION, "decision_epoch": 0, "incumbent": {"candidate_id": "fallback", "feasible": False, "normalized_utility": {"quality": 0.0, "airtime": 0.0, "service": 0.0}, "active_nodes": [], "tiers": {}}, "parameters": default_sacm_parameters()})
            return _rejected(M4_DECISION_V1, decision_id, "invalid-sacm-state", action="fallback", preserved_state_sha256=fallback_state["sha256"]), fallback_state
    try:
        m3 = verify_contract(m3_result, versions={M3_CANDIDATE_RESULT_V1}, required=("status", "selected_candidate_id", "selected_candidate_sha256"))
        if m3["status"] == "infeasible" or m3["selected_candidate_id"] != candidate.get("candidate_id"): raise ContractError("m3-candidate-unavailable")
        supplied_candidate_hash = candidate.get("sha256")
        candidate_hash = supplied_candidate_hash or canonical_sha256(dict(candidate))
        require_hash(candidate_hash, "candidate-sha256")
        if supplied_candidate_hash is not None and candidate_hash != canonical_sha256(dict(candidate)):
            raise ContractError("hash-mismatch")
        if candidate_hash != m3["selected_candidate_sha256"]:
            raise ContractError("selected-candidate-hash-mismatch")
        defaults = default_sacm_parameters()
        if parameters is not None and (not isinstance(parameters, Mapping) or set(parameters) - set(defaults)):
            raise ContractError("invalid-sacm-parameter")
        params = defaults; params.update(parameters or {})
        for key in ("quality_weight", "airtime_weight", "service_weight", "alpha_node", "alpha_rate", "alpha_byte", "epsilon"):
            if not isinstance(params[key], (int, float)) or not math.isfinite(params[key]) or params[key] < 0:
                raise ContractError("invalid-sacm-parameter")
        if abs(params["quality_weight"] + params["airtime_weight"] + params["service_weight"] - 1) > 1e-9: raise ContractError("invalid-utility-weights")
        candidate_id = require_id(candidate.get("candidate_id"), "candidate-id")
        candidate_feasible = candidate.get("feasible") is True
        candidate_inputs = candidate.get("normalized_utility")
        candidate_utility = _normal_utility(candidate_inputs, params)
        prior = preserved_state
        incumbent = prior["incumbent"] if prior else {"candidate_id": "none", "feasible": False, "normalized_utility": {"quality": 0.0, "airtime": 0.0, "service": 0.0}, "active_nodes": [], "tiers": {}}
        incumbent_inputs = incumbent.get("normalized_utility", {"quality": 0.0, "airtime": 0.0, "service": 0.0})
        incumbent_feasible = incumbent.get("feasible") is True
        incumbent_utility = _normal_utility(incumbent_inputs, params)
        universe = max(1, len(set(candidate.get("active_nodes", [])) | set(incumbent.get("active_nodes", []))))
        node_delta = len(set(candidate.get("active_nodes", [])) ^ set(incumbent.get("active_nodes", [])))
        tiers_a, tiers_b = candidate.get("tiers", {}), incumbent.get("tiers", {})
        rate_delta = sum(tiers_a.get(node) != tiers_b.get(node) and node in candidate.get("active_nodes", []) and node in incumbent.get("active_nodes", []) for node in set(tiers_a) | set(tiers_b))
        changed = candidate_id != incumbent.get("candidate_id")
        estimated_bytes = (128 if changed else 0) + 64 * node_delta + 32 * rate_delta
        byte_max = max(1, 128 + 64 * universe + 32 * universe)
        transition = params["alpha_node"] * node_delta / universe + params["alpha_rate"] * rate_delta / universe + params["alpha_byte"] * estimated_bytes / byte_max
        gain = candidate_utility - incumbent_utility
        if candidate_id == incumbent.get("candidate_id") and incumbent_feasible:
            action = "hold"; reason = "unchanged"; accepted = deepcopy(dict(incumbent)); estimated_bytes = 0
        elif candidate_feasible and (not incumbent_feasible or gain >= transition + params["epsilon"]):
            action = "replace"; reason = "incumbent-infeasible" if not incumbent_feasible else "gain-covers-transition"; accepted = deepcopy(dict(candidate))
        elif incumbent_feasible:
            action = "hold"; reason = "candidate-infeasible" if not candidate_feasible else "gain-below-transition"; accepted = deepcopy(dict(incumbent)); estimated_bytes = 0
        else:
            action = "fallback"; reason = "no-feasible-candidate-or-incumbent"; accepted = {"candidate_id": "fallback", "feasible": False, "normalized_utility": {"quality": 0.0, "airtime": 0.0, "service": 0.0}, "active_nodes": [], "tiers": {}}; estimated_bytes = 0
        epoch = int(prior["decision_epoch"]) + 1 if prior else 1
        new_state = serialize_sacm_state({"component_version": SACM_COMPONENT_VERSION, "decision_epoch": epoch, "incumbent": accepted, "parameters": params})
        decision = contract(M4_DECISION_V1, {
            "decision_id": decision_id, "admitted": action != "fallback", "reasons": [reason], "action": action, "reason": reason,
            "decision_epoch": epoch, "m3_result_sha256": m3["sha256"], "candidate_id": candidate_id,
            "incumbent_candidate_id": incumbent.get("candidate_id"), "candidate_feasible": candidate_feasible,
            "incumbent_feasible": incumbent_feasible, "candidate_normalized_utility": candidate_inputs,
            "incumbent_normalized_utility": incumbent_inputs, "candidate_utility": candidate_utility,
            "incumbent_utility": incumbent_utility, "utility_gain": gain, "transition_cost": transition,
            "estimated_bytes": estimated_bytes, "parameters": params, "component_version": SACM_COMPONENT_VERSION,
            "accepted_state_sha256": new_state["sha256"],
        })
        return decision, new_state
    except (ContractError, TypeError, ValueError, OverflowError) as exc:
        fallback_state = preserved_state or serialize_sacm_state({"component_version": SACM_COMPONENT_VERSION, "decision_epoch": 0, "incumbent": {"candidate_id": "fallback", "feasible": False, "normalized_utility": {"quality": 0.0, "airtime": 0.0, "service": 0.0}, "active_nodes": [], "tiers": {}}, "parameters": default_sacm_parameters()})
        reason = str(exc) if isinstance(exc, ContractError) else "invalid-m4-input"
        return _rejected(M4_DECISION_V1, decision_id, reason, action="fallback", preserved_state_sha256=fallback_state["sha256"]), fallback_state


def run_reference_pipeline(*, m1: Mapping[str, Any], m2: Mapping[str, Any], m3: Mapping[str, Any],
                           m4_candidate: Mapping[str, Any] | None = None, state: Mapping[str, Any] | None = None,
                           parameters: Mapping[str, float] | None = None) -> dict[str, Any]:
    """Chain prebuilt stage inputs with a hash-linked ledger; stop at first rejection."""
    ledger: list[dict[str, Any]] = []
    bundle, verdict = m1_admit(**dict(m1)); ledger.extend(({"stage": "m1-bundle", "sha256": bundle["sha256"]}, {"stage": "m1", "sha256": verdict["sha256"], "reasons": verdict["reasons"]}))
    if not verdict["admitted"]: return {"status": "rejected", "failed_stage": "m1", "ledger": ledger, "result": verdict}
    fusion = m2_fuse(bundle=bundle, m1_verdict=verdict, **dict(m2)); ledger.append({"stage": "m2", "sha256": fusion["sha256"], "reasons": fusion["reasons"]})
    if not fusion["admitted"]: return {"status": "rejected", "failed_stage": "m2", "ledger": ledger, "result": fusion}
    m3_values = dict(m3); m3_values.setdefault("evidence_bundle_sha256", bundle["sha256"]); m3_values.setdefault("model_sha256", fusion["model_sha256"])
    selected = m3_select(**m3_values); ledger.append({"stage": "m3", "sha256": selected["sha256"], "reasons": selected["reasons"]})
    if not selected["admitted"]: return {"status": "rejected", "failed_stage": "m3", "ledger": ledger, "result": selected}
    candidate = m4_candidate or next((row for row in m3_values["candidates"] if row["candidate_id"] == selected["selected_candidate_id"]), None)
    if candidate is None: return {"status": "rejected", "failed_stage": "m4", "ledger": ledger, "result": _rejected(M4_DECISION_V1, "m4-missing", "m3-candidate-unavailable")}
    decision, next_state = m4_decide(decision_id="m4-decision", m3_result=selected, candidate=candidate, state=state, parameters=parameters)
    ledger.append({"stage": "m4", "sha256": decision["sha256"], "reasons": decision["reasons"], "accepted_state_sha256": next_state["sha256"]})
    return {"status": "accepted" if decision["admitted"] else "rejected", "failed_stage": None if decision["admitted"] else "m4", "ledger": ledger, "result": decision, "state": next_state}
