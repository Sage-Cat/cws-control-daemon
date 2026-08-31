from __future__ import annotations

import math
from collections import deque

from cws_control.models import TimingEstimate


WRAP_US = 1 << 32


class TimingValidator:
    """Unwrap an ESP local clock and estimate its affine mapping to host time."""

    def __init__(
        self,
        max_points: int = 256,
        max_uncertainty_ms: float = 100.0,
        mode: str = "failure-aware-affine-v1",
    ):
        if mode not in {
            "naive-reset-on-regression",
            "rollover-aware-affine",
            "failure-aware-affine-v1",
        }:
            raise ValueError(f"unsupported timing mode: {mode}")
        self.max_points = max_points
        self.max_uncertainty_ms = max_uncertainty_ms
        self.mode = mode
        self.boot_epoch = 0
        self.reported_boot_epoch: int | None = None
        self.wraps = 0
        self.last_raw_us: int | None = None
        self.points: deque[tuple[int, int]] = deque(maxlen=max_points)

    def reset(self, reported_boot_epoch: int | None, event: str) -> str:
        if reported_boot_epoch is not None:
            self.reported_boot_epoch = reported_boot_epoch
            self.boot_epoch = reported_boot_epoch
        else:
            self.boot_epoch += 1
        self.wraps = 0
        self.last_raw_us = None
        self.points.clear()
        return event

    def observe(
        self,
        raw_timestamp_us: int,
        ingest_monotonic_ns: int,
        reported_boot_epoch: int | None = None,
    ) -> TimingEstimate:
        raw_timestamp_us &= 0xFFFFFFFF
        event: str | None = None
        use_reported_epoch = self.mode == "failure-aware-affine-v1"
        distinguish_wrap = self.mode != "naive-reset-on-regression"
        if use_reported_epoch and reported_boot_epoch is not None and reported_boot_epoch != self.reported_boot_epoch:
            event = self.reset(reported_boot_epoch, "reported-boot-epoch-change")
        elif self.last_raw_us is not None and raw_timestamp_us < self.last_raw_us:
            if (
                distinguish_wrap
                and self.last_raw_us > int(WRAP_US * 0.75)
                and raw_timestamp_us < int(WRAP_US * 0.25)
            ):
                self.wraps += 1
                event = "timestamp-wrap"
            else:
                event = self.reset(
                    None,
                    "raw-regression-reset"
                    if self.mode == "naive-reset-on-regression"
                    else "inferred-clock-reset",
                )
        unwrapped = raw_timestamp_us + self.wraps * WRAP_US
        self.last_raw_us = raw_timestamp_us
        self.points.append((unwrapped, ingest_monotonic_ns))
        drift_ppm, uncertainty_ms = self._fit()
        valid = (
            len(self.points) >= 8
            and drift_ppm is not None
            and uncertainty_ms is not None
            and abs(drift_ppm) <= 10_000.0
            and uncertainty_ms <= self.max_uncertainty_ms
        )
        return TimingEstimate(
            boot_epoch=self.boot_epoch,
            unwrapped_timestamp_us=unwrapped,
            valid=valid,
            points=len(self.points),
            drift_ppm=drift_ppm,
            uncertainty_ms=uncertainty_ms,
            event=event,
        )

    def _fit(self) -> tuple[float | None, float | None]:
        if len(self.points) < 2:
            return None, None
        origin_x, origin_y = self.points[0]
        xs = [float(x - origin_x) for x, _ in self.points]
        ys = [float(y - origin_y) for _, y in self.points]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        denominator = sum((x - mean_x) ** 2 for x in xs)
        if denominator <= 0.0:
            return None, None
        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denominator
        intercept = mean_y - slope * mean_x
        residuals = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
        rmse_ns = math.sqrt(sum(value * value for value in residuals) / len(residuals))
        return (slope / 1000.0 - 1.0) * 1_000_000.0, rmse_ns / 1e6
