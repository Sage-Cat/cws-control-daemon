from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


TIERS = ("off", "low", "medium", "high")


@dataclass(frozen=True)
class Envelope:
    source_id: str
    session_id: str
    connection_epoch: int
    source_sequence: int
    ingest_wall_time_ns: int
    ingest_monotonic_ns: int
    raw: str
    schema_version: str = "cws-source-record/1"
    ingest_sequence: int | None = None
    raw_sha256: str | None = None
    parser_status: str | None = None
    parser_reason: str | None = None
    device_sequence: int | None = None
    boot_epoch: int | None = None
    config_epoch: int | None = None


@dataclass(frozen=True)
class CsiFrame:
    source_id: str
    ingest_wall_time_ns: int
    ingest_monotonic_ns: int
    sequence: int
    mac: str
    rssi: int
    noise_floor: int
    channel: int
    local_timestamp_us: int
    rx_state: int
    first_word_invalid: int
    csi: tuple[int, ...]


@dataclass(frozen=True)
class Heartbeat:
    source_id: str
    ingest_wall_time_ns: int
    ingest_monotonic_ns: int
    values: dict[str, str]

    def integer(self, key: str, default: int | None = None) -> int | None:
        value = self.values.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default


@dataclass(frozen=True)
class TimingEstimate:
    boot_epoch: int
    unwrapped_timestamp_us: int
    valid: bool
    points: int
    drift_ppm: float | None
    uncertainty_ms: float | None
    event: str | None = None


@dataclass(frozen=True)
class NodeEvidence:
    source_id: str
    available: bool
    unavailable_reasons: tuple[str, ...]
    frame_count: int
    expected_frames: float
    rssi_mean: float
    rssi_std: float
    frequency_spread_hz: float
    csi_mean_amplitude: float
    csi_variance: float
    csi_rms: float
    csi_entropy: float
    confidence: float
    reliability: float
    timing_valid: bool
    timing_uncertainty_ms: float | None
    timing_drift_ppm: float | None
    timing_points: int
    trigger: bool
    utility: float
    requested_tier: str
    motion_score: float
    boot_epoch: int | None
    output_drop_ratio: float
    chip_temperature_c: float | None


@dataclass(frozen=True)
class Configuration:
    tiers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        invalid = {node: tier for node, tier in self.tiers.items() if tier not in TIERS}
        if invalid:
            raise ValueError(f"invalid measurement tiers: {invalid}")

    @property
    def active_nodes(self) -> tuple[str, ...]:
        return tuple(sorted(node for node, tier in self.tiers.items() if tier != "off"))

    def tier(self, source_id: str) -> str:
        return self.tiers.get(source_id, "off")


@dataclass(frozen=True)
class BudgetResult:
    feasible: bool
    reason: str
    airtime_percent: float
    qos_penalty_percent: float


@dataclass(frozen=True)
class AqmcResult:
    configuration: Configuration
    budget: BudgetResult
    utilities: dict[str, float]
    requested_tiers: dict[str, str]
    rejected: dict[str, str]
    diagnostic_low_tier: BudgetResult | None = None


@dataclass(frozen=True)
class SacmResult:
    configuration: Configuration
    action: str
    reason: str
    candidate_feasible: bool
    incumbent_feasible: bool
    candidate_utility: float
    incumbent_utility: float
    utility_gain: float
    transition_cost: float
    estimated_update_bytes: int


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def json_ready(value: Any) -> Any:
    """Convert nested dataclass/controller values to JSON-friendly objects."""
    from dataclasses import asdict, is_dataclass

    if is_dataclass(value):
        return {key: json_ready(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    return value
