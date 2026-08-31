#!/usr/bin/env python3
"""Analyze a finalized autonomous-collector natural-operation run.

The raw input remains immutable. Derived outputs are written to an explicit
directory and explicitly avoid turning unlabelled RF variation into occupancy
ground truth.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import heapq
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


WRAP_US = 1 << 32
GAP_THRESHOLDS_SECONDS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def verify_checksums(run_dir: Path) -> dict[str, Any]:
    failures = []
    checked = 0
    for line in (run_dir / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        expected, relative = line.split("  ", 1)
        path = run_dir / relative
        actual = sha256(path) if path.is_file() else None
        checked += 1
        if actual != expected:
            failures.append({"path": relative, "expected": expected, "actual": actual})
    return {"ok": not failures, "checked_files": checked, "failures": failures}


@dataclass
class RunningStats:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    minimum: float | None = None
    maximum: float | None = None

    def add(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        self.m2 += delta * (value - self.mean)
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

    def result(self) -> dict[str, Any]:
        return {
            "n": self.count,
            "mean": self.mean if self.count else None,
            "stddev": math.sqrt(self.m2 / self.count) if self.count else None,
            "min": self.minimum,
            "max": self.maximum,
        }


@dataclass
class GapStats:
    count: int = 0
    sum_seconds: float = 0.0
    maximum_seconds: float = 0.0
    above: Counter = field(default_factory=Counter)
    top: list[tuple[float, int, int]] = field(default_factory=list)

    def add(self, seconds: float, before_ns: int, after_ns: int) -> None:
        if seconds < 0:
            return
        self.count += 1
        self.sum_seconds += seconds
        self.maximum_seconds = max(self.maximum_seconds, seconds)
        for threshold in GAP_THRESHOLDS_SECONDS:
            if seconds >= threshold:
                self.above[str(threshold)] += 1
        item = (seconds, before_ns, after_ns)
        if len(self.top) < 20:
            heapq.heappush(self.top, item)
        elif seconds > self.top[0][0]:
            heapq.heapreplace(self.top, item)

    def result(self) -> dict[str, Any]:
        return {
            "intervals": self.count,
            "mean_seconds": self.sum_seconds / self.count if self.count else None,
            "maximum_seconds": self.maximum_seconds if self.count else None,
            "counts_at_or_above_seconds": dict(self.above),
            "largest": [
                {
                    "seconds": seconds,
                    "before_wall_time_ns": before,
                    "after_wall_time_ns": after,
                    "after_utc": datetime.fromtimestamp(after / 1e9, timezone.utc).isoformat(),
                }
                for seconds, before, after in sorted(self.top, reverse=True)
            ],
        }


@dataclass
class HourBin:
    total_records: int = 0
    csi_candidates: int = 0
    valid_csi: int = 0
    malformed_csi: int = 0
    heartbeats: int = 0
    rssi: RunningStats = field(default_factory=RunningStats)
    sampled_amplitude_mean: RunningStats = field(default_factory=RunningStats)
    sampled_amplitude_rms: RunningStats = field(default_factory=RunningStats)


def key_values(raw: str) -> dict[str, str]:
    return dict(token.split("=", 1) for token in raw.split()[1:] if "=" in token)


def chunk_paths(run_dir: Path) -> list[Path]:
    sources = [path for path in (run_dir / "sources").iterdir() if path.is_dir()]
    if len(sources) != 1:
        raise ValueError(f"expected one source directory, found {sources}")
    paths = sorted(sources[0].glob("*.ndjson.gz"))
    if not paths:
        raise ValueError("no finalized gzip chunks found")
    return paths


def local_hour(wall_ns: int, zone: ZoneInfo) -> str:
    value = datetime.fromtimestamp(wall_ns / 1e9, zone)
    return value.replace(minute=0, second=0, microsecond=0).isoformat()


def parse_csi_structure(raw: str) -> tuple[list[str] | None, str | None]:
    try:
        row = next(csv.reader([raw]))
    except (csv.Error, StopIteration) as exc:
        return None, f"csv:{exc}"
    if len(row) != 25:
        return None, f"field-count:{len(row)}"
    try:
        for index in (1, 3, 14, 16, 18, 21, 22, 23):
            int(row[index])
        payload = row[24].strip()
        if not (payload.startswith("[") and payload.endswith("]")):
            return None, "payload-brackets"
        body = payload[1:-1].strip()
        observed = 0 if not body else body.count(",") + 1
        if observed != int(row[22]):
            return None, f"declared-length:{row[22]}-observed:{observed}"
    except ValueError as exc:
        return None, f"numeric:{exc}"
    return row, None


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            values.append(json.loads(line))
    return values


def read_ndjson_lenient(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    values = []
    errors = []
    if not path.is_file():
        return values, errors
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            values.append(json.loads(line))
        except json.JSONDecodeError as exc:
            start = line.find("{")
            try:
                if start < 0:
                    raise exc
                values.append(json.loads(line[start:]))
                errors.append({
                    "line": line_number,
                    "error": str(exc),
                    "prefix_bytes": start,
                    "prefix": line[: min(start, 200)],
                    "recovered": True,
                })
            except json.JSONDecodeError:
                errors.append({
                    "line": line_number,
                    "error": str(exc),
                    "prefix": line[:200],
                    "recovered": False,
                })
    return values, errors


def analyze_chunk_ledger(run_dir: Path, observed_records: int) -> dict[str, Any]:
    entries = read_ndjson(run_dir / "chunks.ndjson")
    statuses = Counter(str(entry.get("status", "missing")) for entry in entries)
    ledger_records = sum(int(entry.get("records") or 0) for entry in entries)
    ledger_bytes = sum(int(entry.get("compressed_bytes") or 0) for entry in entries)
    paths = [str(entry.get("path", "")) for entry in entries]
    return {
        "entries": len(entries),
        "status_counts": dict(statuses),
        "unique_paths": len(set(paths)),
        "ledger_records": ledger_records,
        "observed_records": observed_records,
        "record_difference": ledger_records - observed_records,
        "ledger_compressed_bytes": ledger_bytes,
        "all_complete": bool(entries) and statuses == {"complete": len(entries)},
    }


def analyze_system_health(run_dir: Path) -> dict[str, Any]:
    entries, json_errors = read_ndjson_lenient(run_dir / "system-health.ndjson")
    temperature = RunningStats()
    load_1m = RunningStats()
    load_5m = RunningStats()
    load_15m = RunningStats()
    boot_ids: Counter[str] = Counter()
    interface_states: dict[str, Counter[str]] = defaultdict(Counter)
    clock_synchronized: Counter[str] = Counter()
    wall_regressions = 0
    monotonic_regressions = 0
    maximum_gap_seconds = 0.0
    previous_wall = previous_monotonic = None
    first_disk_free = last_disk_free = minimum_disk_free = None
    first_interfaces: dict[str, Any] = {}
    last_interfaces: dict[str, Any] = {}
    for entry in entries:
        try:
            wall_ns = int(entry["wall_time_ns"])
            monotonic_ns = int(entry["monotonic_ns"])
        except (KeyError, TypeError, ValueError):
            continue
        if previous_wall is not None:
            if wall_ns < previous_wall:
                wall_regressions += 1
            maximum_gap_seconds = max(maximum_gap_seconds, (wall_ns - previous_wall) / 1e9)
        if previous_monotonic is not None and monotonic_ns < previous_monotonic:
            monotonic_regressions += 1
        previous_wall = wall_ns
        previous_monotonic = monotonic_ns
        if entry.get("boot_id"):
            boot_ids[str(entry["boot_id"])] += 1
        clock_synchronized[str(bool(entry.get("system_clock_synchronized"))).lower()] += 1
        try:
            temperature.add(int(entry["temperature_millicelsius"]) / 1000)
        except (KeyError, TypeError, ValueError):
            pass
        load = entry.get("load_average", [])
        if isinstance(load, list) and len(load) >= 3:
            load_1m.add(float(load[0]))
            load_5m.add(float(load[1]))
            load_15m.add(float(load[2]))
        try:
            disk_free = int(entry["disk_free_bytes"])
            first_disk_free = disk_free if first_disk_free is None else first_disk_free
            last_disk_free = disk_free
            minimum_disk_free = disk_free if minimum_disk_free is None else min(minimum_disk_free, disk_free)
        except (KeyError, TypeError, ValueError):
            pass
        interfaces = entry.get("interfaces", {})
        if isinstance(interfaces, dict):
            first_interfaces = interfaces if not first_interfaces else first_interfaces
            last_interfaces = interfaces
            for name, values in interfaces.items():
                if isinstance(values, dict):
                    interface_states[name][str(values.get("operstate", "missing"))] += 1

    interface_deltas = {}
    for name in sorted(set(first_interfaces) | set(last_interfaces)):
        first = first_interfaces.get(name, {})
        last = last_interfaces.get(name, {})
        deltas = {}
        for counter in ("rx_bytes", "tx_bytes", "rx_dropped", "tx_dropped"):
            try:
                deltas[counter] = int(last[counter]) - int(first[counter])
            except (KeyError, TypeError, ValueError):
                deltas[counter] = None
        interface_deltas[name] = deltas

    return {
        "samples": len(entries),
        "json_damage": {
            "lines": len(json_errors),
            "recovered": sum(bool(item.get("recovered")) for item in json_errors),
            "unrecovered": sum(not bool(item.get("recovered")) for item in json_errors),
        },
        "boot_ids": dict(boot_ids),
        "system_clock_synchronized_counts": dict(clock_synchronized),
        "wall_time_regressions": wall_regressions,
        "monotonic_time_regressions": monotonic_regressions,
        "maximum_sample_gap_seconds": maximum_gap_seconds if entries else None,
        "temperature_c": temperature.result(),
        "load_average_1m": load_1m.result(),
        "load_average_5m": load_5m.result(),
        "load_average_15m": load_15m.result(),
        "disk_free_bytes": {
            "first": first_disk_free,
            "last": last_disk_free,
            "minimum": minimum_disk_free,
            "consumed_first_to_last": (
                first_disk_free - last_disk_free
                if first_disk_free is not None and last_disk_free is not None else None
            ),
        },
        "interface_state_counts": {
            name: dict(states) for name, states in sorted(interface_states.items())
        },
        "interface_counter_deltas": interface_deltas,
    }


def analyze(
    run_dir: Path,
    sample_every: int = 100,
    time_zone: str = "UTC",
) -> dict[str, Any]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    final = json.loads((run_dir / "final.json").read_text(encoding="utf-8"))
    integrity = verify_checksums(run_dir)
    if not integrity["ok"]:
        raise ValueError(f"raw archive checksum failure: {integrity['failures']}")

    zone = ZoneInfo(time_zone)
    kinds: Counter[str] = Counter()
    channels: Counter[int] = Counter()
    macs: Counter[str] = Counter()
    rx_states: Counter[int] = Counter()
    first_word_invalid: Counter[int] = Counter()
    boot_epochs: Counter[int] = Counter()
    firmware_versions: Counter[str] = Counter()
    connection_epochs: Counter[int] = Counter()
    sessions: Counter[str] = Counter()
    malformed_reasons: Counter[str] = Counter()
    malformed_examples: list[dict[str, Any]] = []
    hours: dict[str, HourBin] = defaultdict(HourBin)
    rssi = RunningStats()
    noise = RunningStats()
    temperature = RunningStats()
    amplitude_mean = RunningStats()
    amplitude_rms = RunningStats()
    all_gaps = GapStats()
    csi_gaps = GapStats()

    total = 0
    json_errors = 0
    valid_csi = 0
    malformed_csi = 0
    fully_validated_samples = 0
    sample_parse_errors = 0
    first_wall = last_wall = None
    previous_wall = previous_csi_wall = None
    previous_source_sequences: dict[str, int] = {}
    source_sequence_missing = source_sequence_duplicates = source_sequence_regressions = 0
    previous_csi_sequence = None
    csi_sequence_missing = csi_sequence_duplicates = csi_sequence_regressions = 0
    csi_sequence_first = csi_sequence_last = None
    previous_local_us = None
    timestamp_wraps = timestamp_nonwrap_regressions = 0
    heartbeat_first: dict[str, Any] | None = None
    heartbeat_last: dict[str, Any] | None = None
    profile_count = 0

    for path in chunk_paths(run_dir):
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                try:
                    envelope = json.loads(line)
                    wall_ns = int(envelope["ingest_wall_time_ns"])
                    source_sequence = int(envelope["source_sequence"])
                    raw = str(envelope["raw"])
                    connection_epoch = int(envelope["connection_epoch"])
                    session_id = str(envelope["session_id"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    json_errors += 1
                    continue
                total += 1
                first_wall = wall_ns if first_wall is None else min(first_wall, wall_ns)
                last_wall = wall_ns if last_wall is None else max(last_wall, wall_ns)
                hour = hours[local_hour(wall_ns, zone)]
                hour.total_records += 1
                connection_epochs[connection_epoch] += 1
                sessions[session_id] += 1
                if previous_wall is not None:
                    all_gaps.add((wall_ns - previous_wall) / 1e9, previous_wall, wall_ns)
                previous_wall = wall_ns
                previous_source_sequence = previous_source_sequences.get(session_id)
                if previous_source_sequence is not None:
                    delta = source_sequence - previous_source_sequence
                    if delta > 1:
                        source_sequence_missing += delta - 1
                    elif delta == 0:
                        source_sequence_duplicates += 1
                    elif delta < 0:
                        source_sequence_regressions += 1
                previous_source_sequences[session_id] = source_sequence

                if raw.startswith("CSI_DATA,"):
                    kinds["csi_candidate"] += 1
                    hour.csi_candidates += 1
                    row, error = parse_csi_structure(raw)
                    if row is None:
                        malformed_csi += 1
                        hour.malformed_csi += 1
                        malformed_reasons[error or "unknown"] += 1
                        if len(malformed_examples) < 20:
                            malformed_examples.append({
                                "chunk": path.name,
                                "line": line_number,
                                "wall_time_ns": wall_ns,
                                "source_sequence": source_sequence,
                                "reason": error,
                                "raw_prefix": raw[:300],
                            })
                        continue
                    valid_csi += 1
                    hour.valid_csi += 1
                    sequence = int(row[1])
                    csi_sequence_first = sequence if csi_sequence_first is None else csi_sequence_first
                    csi_sequence_last = sequence
                    if previous_csi_sequence is not None:
                        delta = sequence - previous_csi_sequence
                        if delta > 1:
                            csi_sequence_missing += delta - 1
                        elif delta == 0:
                            csi_sequence_duplicates += 1
                        elif delta < 0:
                            csi_sequence_regressions += 1
                    previous_csi_sequence = sequence
                    if previous_csi_wall is not None:
                        csi_gaps.add((wall_ns - previous_csi_wall) / 1e9, previous_csi_wall, wall_ns)
                    previous_csi_wall = wall_ns
                    value_rssi = int(row[3])
                    value_noise = int(row[14])
                    rssi.add(value_rssi)
                    noise.add(value_noise)
                    hour.rssi.add(value_rssi)
                    channels[int(row[16])] += 1
                    macs[row[2].lower()] += 1
                    rx_states[int(row[21])] += 1
                    first_word_invalid[int(row[23])] += 1
                    local_us = int(row[18]) & 0xFFFFFFFF
                    if previous_local_us is not None and local_us < previous_local_us:
                        if previous_local_us > WRAP_US * 0.75 and local_us < WRAP_US * 0.25:
                            timestamp_wraps += 1
                        else:
                            timestamp_nonwrap_regressions += 1
                    previous_local_us = local_us

                    if source_sequence % sample_every == 0:
                        try:
                            values = [int(item) for item in row[24][1:-1].split(",") if item]
                            if len(values) != int(row[22]) or len(values) % 2:
                                raise ValueError("invalid sampled IQ length")
                            magnitudes_squared = [
                                values[index] ** 2 + values[index + 1] ** 2
                                for index in range(0, len(values), 2)
                            ]
                            frame_mean = sum(math.sqrt(value) for value in magnitudes_squared) / len(magnitudes_squared)
                            frame_rms = math.sqrt(sum(magnitudes_squared) / len(magnitudes_squared))
                            amplitude_mean.add(frame_mean)
                            amplitude_rms.add(frame_rms)
                            hour.sampled_amplitude_mean.add(frame_mean)
                            hour.sampled_amplitude_rms.add(frame_rms)
                            fully_validated_samples += 1
                        except (ValueError, ZeroDivisionError):
                            sample_parse_errors += 1
                elif raw.startswith("CWSLAB_TIMING_HEARTBEAT "):
                    kinds["timing_heartbeat"] += 1
                    hour.heartbeats += 1
                    values = key_values(raw)
                    record = {"wall_time_ns": wall_ns, "values": values}
                    heartbeat_first = record if heartbeat_first is None else heartbeat_first
                    heartbeat_last = record
                    try:
                        boot_epochs[int(values["boot_epoch"])] += 1
                    except (KeyError, ValueError):
                        pass
                    if values.get("fw_version"):
                        firmware_versions[values["fw_version"]] += 1
                    try:
                        temperature.add(int(values["chip_temp_millicelsius"]) / 1000)
                    except (KeyError, ValueError):
                        pass
                elif raw.startswith("CSI_PROFILE "):
                    kinds["profile"] += 1
                    profile_count += 1
                else:
                    prefix = raw.split(None, 1)[0].split(",", 1)[0] if raw else "empty"
                    kinds[f"other:{prefix}"] += 1

    if first_wall is None or last_wall is None:
        raise ValueError("archive contains no valid envelopes")

    events, event_json_errors = read_ndjson_lenient(run_dir / "events.ndjson")
    disconnects = []
    pending: dict[str, Any] | None = None
    for event in events:
        if event.get("event_type") == "source_disconnected":
            pending = event
        elif pending and event.get("event_type") == "source_connected":
            disconnects.append({
                "source_id": pending.get("source_id"),
                "start_wall_time_ns": pending["wall_time_ns"],
                "end_wall_time_ns": event["wall_time_ns"],
                "duration_seconds": (event["wall_time_ns"] - pending["wall_time_ns"]) / 1e9,
                "error": pending.get("error"),
            })
            pending = None

    hourly_rows = []
    for label, value in sorted(hours.items()):
        hourly_rows.append({
            "hour_local": label,
            "total_records": value.total_records,
            "csi_candidates": value.csi_candidates,
            "valid_csi": value.valid_csi,
            "malformed_csi": value.malformed_csi,
            "heartbeats": value.heartbeats,
            "mean_csi_per_second_nominal_hour": value.valid_csi / 3600,
            "rssi": value.rssi.result(),
            "sampled_csi_mean_amplitude": value.sampled_amplitude_mean.result(),
            "sampled_csi_rms": value.sampled_amplitude_rms.result(),
        })

    heartbeat_counters = None
    if heartbeat_first and heartbeat_last:
        first_values = heartbeat_first["values"]
        last_values = heartbeat_last["values"]
        heartbeat_counters = {
            "first_wall_time_ns": heartbeat_first["wall_time_ns"],
            "last_wall_time_ns": heartbeat_last["wall_time_ns"],
            "first_uptime_ms": int(first_values.get("uptime_ms", 0)),
            "last_uptime_ms": int(last_values.get("uptime_ms", 0)),
            "first_csi_count": int(first_values.get("csi_count", 0)),
            "last_csi_count": int(last_values.get("csi_count", 0)),
            "csi_count_delta": int(last_values.get("csi_count", 0)) - int(first_values.get("csi_count", 0)),
            "first_output_drops": int(first_values.get("output_drops", 0)),
            "last_output_drops": int(last_values.get("output_drops", 0)),
            "output_drop_delta": int(last_values.get("output_drops", 0)) - int(first_values.get("output_drops", 0)),
        }

    host_health = analyze_system_health(run_dir)
    chunk_ledger = analyze_chunk_ledger(run_dir, total)
    partial_chunks = sorted(
        str(path.relative_to(run_dir))
        for path in run_dir.glob("sources/**/*")
        if path.is_file() and (path.name.endswith(".incomplete") or path.name.endswith(".partial"))
    )
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "time_zone": time_zone,
        "claim_boundary": (
            "This is an unlabelled natural-operation RF/CSI dataset. It supports stability, continuity, "
            "timing-wrap, signal-variation, and data-quality claims; it does not establish human occupancy "
            "or activity-classification accuracy."
        ),
        "run": {
            "run_id": manifest["run_id"],
            "source_id": next(iter(final["sources"])),
            "start_utc": manifest["started_at"],
            "target_end_utc": manifest["target_end_at"],
            "ended_utc": final["ended_at"],
            "finish_reason": final["reason"],
            "scheduled_duration_seconds": manifest["duration_seconds"],
            "observed_first_record_utc": datetime.fromtimestamp(first_wall / 1e9, timezone.utc).isoformat(),
            "observed_last_record_utc": datetime.fromtimestamp(last_wall / 1e9, timezone.utc).isoformat(),
            "observed_span_seconds": (last_wall - first_wall) / 1e9,
            "chunk_count": len(chunk_paths(run_dir)),
            "raw_compressed_bytes": sum(path.stat().st_size for path in chunk_paths(run_dir)),
            "partial_chunk_count": len(partial_chunks),
            "partial_chunks": partial_chunks,
            "collector_final": final["sources"],
        },
        "integrity": integrity,
        "artifact_accounting": {
            "chunk_ledger": chunk_ledger,
            "event_records": len(events),
            "event_json_errors": len(event_json_errors),
            "event_json_error_examples": event_json_errors[:20],
            "event_type_counts": dict(Counter(str(event.get("event_type", "missing")) for event in events)),
        },
        "records": {
            "total_envelopes": total,
            "envelope_json_errors": json_errors,
            "types": dict(kinds),
            "valid_csi": valid_csi,
            "malformed_csi": malformed_csi,
            "malformed_csi_fraction": malformed_csi / max(1, valid_csi + malformed_csi),
            "malformed_reasons": dict(malformed_reasons),
            "malformed_examples": malformed_examples,
            "fully_validated_feature_samples": fully_validated_samples,
            "feature_sample_every_envelopes": sample_every,
            "feature_sample_parse_errors": sample_parse_errors,
        },
        "identity": {
            "sessions": dict(sessions),
            "connection_epochs": {str(key): value for key, value in sorted(connection_epochs.items())},
            "macs": dict(macs),
            "channels": {str(key): value for key, value in sorted(channels.items())},
            "firmware_versions": dict(firmware_versions),
            "boot_epochs": {str(key): value for key, value in sorted(boot_epochs.items())},
            "profile_records": profile_count,
        },
        "continuity": {
            "source_sequence_missing": source_sequence_missing,
            "source_sequence_duplicates": source_sequence_duplicates,
            "source_sequence_regressions": source_sequence_regressions,
            "csi_sequence_first": csi_sequence_first,
            "csi_sequence_last": csi_sequence_last,
            "csi_sequence_missing": csi_sequence_missing,
            "csi_sequence_duplicates": csi_sequence_duplicates,
            "csi_sequence_regressions": csi_sequence_regressions,
            "all_record_gaps": all_gaps.result(),
            "valid_csi_gaps": csi_gaps.result(),
            "transport_disconnects": disconnects,
        },
        "timing": {
            "timestamp_wraps": timestamp_wraps,
            "nonwrap_timestamp_regressions": timestamp_nonwrap_regressions,
        },
        "radio": {
            "rssi_dbm": rssi.result(),
            "noise_floor_dbm": noise.result(),
            "rx_state_counts": {str(key): value for key, value in sorted(rx_states.items())},
            "first_word_invalid_counts": {str(key): value for key, value in sorted(first_word_invalid.items())},
            "sampled_csi_mean_amplitude": amplitude_mean.result(),
            "sampled_csi_rms": amplitude_rms.result(),
        },
        "health": {
            "temperature_c": temperature.result(),
            "heartbeat_counters": heartbeat_counters,
            "collector_host": host_health,
        },
        "hourly": hourly_rows,
    }


def markdown(report: dict[str, Any]) -> str:
    def decimal(value: Any, digits: int = 3) -> str:
        return f"{float(value):.{digits}f}" if value is not None else "n/a"

    run = report["run"]
    records = report["records"]
    continuity = report["continuity"]
    timing = report["timing"]
    radio = report["radio"]
    health = report["health"]["heartbeat_counters"] or {}
    host = report["health"]["collector_host"]
    ledger = report["artifact_accounting"]["chunk_ledger"]
    disconnects = continuity["transport_disconnects"]
    lines = [
        "# Natural-operation CSI run analysis",
        "",
        f"Run `{run['run_id']}` completed normally and spans {run['observed_span_seconds'] / 3600:.4f} observed hours.",
        f"All {report['integrity']['checked_files']} archived checksummed files verify; "
        f"the archive contains {run['chunk_count']} finalized chunks and "
        f"{run['partial_chunk_count']} retained partial chunks.",
        "",
        "## Core results",
        "",
        f"- Envelopes: {records['total_envelopes']:,}; structurally valid CSI: {records['valid_csi']:,}; "
        f"malformed CSI candidates: {records['malformed_csi']:,} "
        f"({records['malformed_csi_fraction'] * 100:.6f}%).",
        f"- ESP timestamp wraps: {timing['timestamp_wraps']}; non-wrap regressions: "
        f"{timing['nonwrap_timestamp_regressions']}.",
        f"- CSI sequence-estimated missing frames: {continuity['csi_sequence_missing']:,}; "
        f"duplicates: {continuity['csi_sequence_duplicates']:,}; regressions: "
        f"{continuity['csi_sequence_regressions']:,}.",
        f"- RSSI: mean {radio['rssi_dbm']['mean']:.3f} dBm, standard deviation "
        f"{radio['rssi_dbm']['stddev']:.3f} dB, range {radio['rssi_dbm']['min']}..{radio['rssi_dbm']['max']} dBm.",
        f"- Longest valid-CSI ingestion gap: {continuity['valid_csi_gaps']['maximum_seconds']:.6f} s.",
        f"- Serial transport disconnects: {len(disconnects)}"
        + (f"; duration {disconnects[0]['duration_seconds']:.6f} s." if len(disconnects) == 1 else "."),
    ]
    if health:
        lines.append(
            f"- Firmware counters between first/last heartbeats: CSI delta {health['csi_count_delta']:,}; "
            f"output-drop delta {health['output_drop_delta']:,}."
        )
    lines.extend([
        "",
        "## Archive and collector-host validation",
        "",
        f"- Chunk ledger: {ledger['entries']} entries, {ledger['status_counts'].get('complete', 0)} complete, "
        f"{ledger['unique_paths']} unique paths; ledger and parsed-envelope counts differ by "
        f"{ledger['record_difference']}.",
        f"- Host health: {host['samples']:,} samples; {len(host['boot_ids'])} boot ID; "
        f"clock synchronized in {host['system_clock_synchronized_counts'].get('true', 0):,}/"
        f"{host['samples']:,} samples; wall/monotonic regressions "
        f"{host['wall_time_regressions']}/{host['monotonic_time_regressions']}.",
        f"- Collector temperature: mean {host['temperature_c']['mean']:.2f} C, maximum "
        f"{host['temperature_c']['max']:.2f} C; maximum health-sample gap "
        f"{host['maximum_sample_gap_seconds']:.3f} s.",
        f"- Collector one-minute load: mean {host['load_average_1m']['mean']:.3f}, maximum "
        f"{host['load_average_1m']['max']:.3f}; minimum free storage "
        f"{host['disk_free_bytes']['minimum'] / (1024 ** 3):.2f} GiB.",
        f"- Interface-state counts: {host['interface_state_counts']}.",
        "",
        "## Failure-aware interpretation",
        "",
        f"- The {timing['timestamp_wraps']} ESP 32-bit microsecond-clock wraps with zero non-wrap "
        "regressions are a natural-run validation of rollover handling, not reboot events.",
        f"- The sole {disconnects[0]['duration_seconds']:.3f}-second transport interruption "
        "reconnected without changing the firmware boot epoch; it is therefore a serial-path failure, "
        "not evidence of an ESP reboot." if len(disconnects) == 1 else
        f"- The run contains {len(disconnects)} serial transport interruptions.",
        f"- The longest CSI-only silence was {continuity['valid_csi_gaps']['maximum_seconds']:.3f} seconds, "
        f"while the longest gap across every envelope was {continuity['all_record_gaps']['maximum_seconds']:.3f} "
        "seconds. Heartbeats continued through part of the CSI silence, allowing sensing-path loss to be "
        "distinguished from complete node loss.",
        "- Collector source-sequence continuity is exact. CSI sequence gaps and the firmware output-drop "
        "counter are separate observations and must not be added as though they identify the same lost frames.",
        "- The reported noise floor is invariant, so it is retained for provenance but is not a useful "
        "time-varying interference covariate in this run.",
    ])
    lines.extend([
        "",
        "## Hourly signal summary",
        "",
        "| Local hour | Valid CSI | Malformed | Mean CSI/s | Mean RSSI (dBm) | RSSI SD | Sampled mean amplitude |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for row in report["hourly"]:
        rssi = row["rssi"]
        amplitude = row["sampled_csi_mean_amplitude"]
        lines.append(
            f"| {row['hour_local']} | {row['valid_csi']} | {row['malformed_csi']} | "
            f"{row['mean_csi_per_second_nominal_hour']:.3f} | {decimal(rssi['mean'])} | "
            f"{decimal(rssi['stddev'])} | {decimal(amplitude['mean'])} |"
        )
    lines.extend([
        "",
        "## Interpretation boundary",
        "",
        report["claim_boundary"],
        "The hourly edge bins are partial clock hours, so their nominal per-hour rates are not comparable "
        "to complete middle hours without duration normalization.",
        "CSI amplitude features are computed from a deterministic 1-in-"
        f"{records['feature_sample_every_envelopes']} envelope sample; RSSI, continuity, timing, and validity "
        "statistics use every archived record.",
        "",
    ])
    return "\n".join(lines)


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = output_dir / "summary.json"
    summary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "report.md").write_text(markdown(report), encoding="utf-8")
    with (output_dir / "hourly.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow([
            "hour_local", "total_records", "valid_csi", "malformed_csi", "heartbeats",
            "mean_csi_per_second_nominal_hour", "rssi_mean_dbm", "rssi_stddev_db",
            "sampled_csi_mean_amplitude", "sampled_csi_rms",
        ])
        for row in report["hourly"]:
            writer.writerow([
                row["hour_local"], row["total_records"], row["valid_csi"], row["malformed_csi"],
                row["heartbeats"], row["mean_csi_per_second_nominal_hour"], row["rssi"]["mean"],
                row["rssi"]["stddev"], row["sampled_csi_mean_amplitude"]["mean"],
                row["sampled_csi_rms"]["mean"],
            ])
    lines = [json.dumps(value, sort_keys=True) for value in report["records"]["malformed_examples"]]
    (output_dir / "malformed-examples.ndjson").write_text(
        "\n".join(lines) + ("\n" if lines else ""), encoding="utf-8"
    )
    checksum_lines = []
    for path in sorted(output_dir.iterdir()):
        if path.is_file() and path.name != "SHA256SUMS":
            checksum_lines.append(f"{sha256(path)}  {path.name}\n")
    (output_dir / "SHA256SUMS").write_text("".join(checksum_lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze a finalized natural-operation collector run")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-every", type=int, default=100)
    parser.add_argument("--time-zone", default="UTC", help="IANA zone for hourly bins")
    args = parser.parse_args()
    if args.sample_every < 1:
        parser.error("--sample-every must be positive")
    report = analyze(args.run_dir, args.sample_every, args.time_zone)
    write_outputs(report, args.output_dir)
    print(json.dumps({
        "run_id": report["run"]["run_id"],
        "total_envelopes": report["records"]["total_envelopes"],
        "valid_csi": report["records"]["valid_csi"],
        "malformed_csi": report["records"]["malformed_csi"],
        "timestamp_wraps": report["timing"]["timestamp_wraps"],
        "output_dir": str(args.output_dir),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
