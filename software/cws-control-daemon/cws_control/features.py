from __future__ import annotations

import cmath
import math
from statistics import fmean, pstdev, pvariance
from typing import Sequence

from cws_control.models import CsiFrame


def iq_to_amplitude(csi: Sequence[int]) -> list[float]:
    usable = len(csi) - len(csi) % 2
    return [math.hypot(csi[index], csi[index + 1]) for index in range(0, usable, 2)]


def histogram_entropy(values: Sequence[float], bins: int = 16) -> float:
    if not values:
        return 0.0
    low = min(values)
    high = max(values)
    if high <= low:
        return 0.0
    counts = [0] * bins
    scale = bins / (high - low)
    for value in values:
        index = min(bins - 1, int((value - low) * scale))
        counts[index] += 1
    total = len(values)
    return -sum((count / total) * math.log2(count / total) for count in counts if count)


def amplitude_features(frames: Sequence[CsiFrame]) -> tuple[float, float, float, float]:
    amplitudes = [amplitude for frame in frames for amplitude in iq_to_amplitude(frame.csi)]
    if not amplitudes:
        return 0.0, 0.0, 0.0, 0.0
    mean = fmean(amplitudes)
    variance = pvariance(amplitudes) if len(amplitudes) > 1 else 0.0
    rms = math.sqrt(fmean(value * value for value in amplitudes))
    return mean, variance, rms, histogram_entropy(amplitudes)


def rssi_features(frames: Sequence[CsiFrame]) -> tuple[float, float]:
    values = [float(frame.rssi) for frame in frames]
    if not values:
        return 0.0, 0.0
    return fmean(values), pstdev(values) if len(values) > 1 else 0.0


def frequency_spread_hz(frames: Sequence[CsiFrame], max_points: int = 64) -> float:
    """Return power-weighted RSSI spectral spread around the spectral centroid.

    The AQ-MC publication defines rho as a frequency-domain RSSI spread
    descriptor and normalizes it by 5. This implementation makes the descriptor
    explicit: population standard deviation of positive DFT-bin frequencies,
    weighted by demeaned RSSI power.
    """
    if len(frames) < 4:
        return 0.0
    sampled = list(frames[-max_points:])
    rssis = [float(frame.rssi) for frame in sampled]
    mean = fmean(rssis)
    centered = [value - mean for value in rssis]
    span_ns = sampled[-1].ingest_wall_time_ns - sampled[0].ingest_wall_time_ns
    if span_ns <= 0:
        return 0.0
    sample_rate = (len(sampled) - 1) * 1e9 / span_ns
    count = len(centered)
    frequencies: list[float] = []
    powers: list[float] = []
    for index in range(1, count // 2 + 1):
        value = sum(
            centered[position] * cmath.exp(-2j * math.pi * index * position / count)
            for position in range(count)
        )
        power = value.real * value.real + value.imag * value.imag
        frequencies.append(index * sample_rate / count)
        powers.append(power)
    total = sum(powers)
    if total <= 0.0:
        return 0.0
    centroid = sum(freq * power for freq, power in zip(frequencies, powers)) / total
    variance = sum(power * (freq - centroid) ** 2 for freq, power in zip(frequencies, powers)) / total
    return math.sqrt(max(0.0, variance))
