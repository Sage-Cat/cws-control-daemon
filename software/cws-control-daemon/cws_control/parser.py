from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
from typing import Any

from cws_control.models import CsiFrame, Envelope, Heartbeat


class ParseError(ValueError):
    """Raised when a collector envelope or sensing record is malformed."""


SOURCE_RECORD_V1 = "cws-source-record/1"
SOURCE_RECORD_V2 = "cws-source-record/2"
_SOURCE_ID = re.compile(r"[a-z0-9][a-z0-9-]{1,62}\Z")
_SESSION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}\Z")
_LOWER_KEBAB = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")


def _integer_field(value: Any, field: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ParseError(f"invalid-{field}")
    if maximum is not None and value > maximum:
        raise ParseError(f"invalid-{field}")
    return value


def _nullable_integer(
    value: Any,
    field: str,
    maximum: int,
) -> int | None:
    if value is None:
        return None
    return _integer_field(value, field, 0, maximum)


def parse_envelope(value: str | bytes | dict[str, Any]) -> Envelope:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ParseError(f"invalid envelope JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ParseError("envelope must be a JSON object")
    try:
        version = value.get("schema_version", SOURCE_RECORD_V1)
        if version not in {SOURCE_RECORD_V1, SOURCE_RECORD_V2}:
            raise ParseError("unknown-source-record-major")
        source_id = value["source_id"]
        session_id = value["session_id"]
        if not isinstance(source_id, str) or not _SOURCE_ID.fullmatch(source_id):
            raise ParseError("invalid-source-id")
        if not isinstance(session_id, str) or not _SESSION_ID.fullmatch(session_id):
            raise ParseError("invalid-session-id")
        raw = value["raw"]
        if not isinstance(raw, str):
            raise ParseError("invalid-raw")
        connection_epoch = _integer_field(value["connection_epoch"], "connection-epoch", 1)
        source_sequence = _integer_field(value["source_sequence"], "source-sequence", 1)
        ingest_wall_time_ns = _integer_field(value["ingest_wall_time_ns"], "ingest-wall-time-ns", 0)
        ingest_monotonic_ns = _integer_field(
            value["ingest_monotonic_ns"], "ingest-monotonic-ns", 0
        )
        ingest_sequence = raw_sha256 = parser_status = parser_reason = None
        device_sequence = boot_epoch = config_epoch = None
        if version == SOURCE_RECORD_V2:
            required = {
                "ingest_sequence",
                "raw_sha256",
                "parser_status",
                "parser_reason",
                "device_sequence",
                "boot_epoch",
                "config_epoch",
            }
            missing = sorted(required - value.keys())
            if missing:
                raise ParseError(f"missing-v2-field:{missing[0]}")
            ingest_sequence = _integer_field(value["ingest_sequence"], "ingest-sequence", 1)
            raw_sha256 = value["raw_sha256"]
            if (
                not isinstance(raw_sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", raw_sha256)
                or raw_sha256 != hashlib.sha256(raw.encode("utf-8")).hexdigest()
            ):
                raise ParseError("raw-hash-mismatch")
            parser_status = value["parser_status"]
            if parser_status not in {"parsed", "unknown", "malformed"}:
                raise ParseError("invalid-parser-status")
            parser_reason = value["parser_reason"]
            if not isinstance(parser_reason, str) or not _LOWER_KEBAB.fullmatch(parser_reason):
                raise ParseError("invalid-parser-reason")
            device_sequence = _nullable_integer(
                value["device_sequence"], "device-sequence", (1 << 64) - 1
            )
            boot_epoch = _nullable_integer(value["boot_epoch"], "boot-epoch", (1 << 32) - 1)
            config_epoch = _nullable_integer(
                value["config_epoch"], "config-epoch", (1 << 32) - 1
            )
        return Envelope(
            source_id=source_id,
            session_id=session_id,
            connection_epoch=connection_epoch,
            source_sequence=source_sequence,
            ingest_wall_time_ns=ingest_wall_time_ns,
            ingest_monotonic_ns=ingest_monotonic_ns,
            raw=raw,
            schema_version=version,
            ingest_sequence=ingest_sequence,
            raw_sha256=raw_sha256,
            parser_status=parser_status,
            parser_reason=parser_reason,
            device_sequence=device_sequence,
            boot_epoch=boot_epoch,
            config_epoch=config_epoch,
        )
    except ParseError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid envelope fields: {exc}") from exc


def parse_key_values(raw: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for token in raw.split()[1:]:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if key:
            values[key] = value
    return values


def parse_csi(envelope: Envelope) -> CsiFrame:
    try:
        row = next(csv.reader([envelope.raw]))
    except (csv.Error, StopIteration) as exc:
        raise ParseError(f"invalid CSI CSV: {exc}") from exc
    if len(row) != 25 or row[0] != "CSI_DATA":
        raise ParseError(f"expected 25 CSI_DATA fields, got {len(row)}")
    try:
        values = ast.literal_eval(row[24])
        if not isinstance(values, list):
            raise ValueError("CSI payload is not a list")
        csi = tuple(int(value) for value in values)
        declared_length = int(row[22])
        if declared_length != len(csi):
            raise ValueError(f"declared CSI length {declared_length} != {len(csi)}")
        return CsiFrame(
            source_id=envelope.source_id,
            ingest_wall_time_ns=envelope.ingest_wall_time_ns,
            ingest_monotonic_ns=envelope.ingest_monotonic_ns,
            sequence=int(row[1]),
            mac=row[2].lower(),
            rssi=int(row[3]),
            noise_floor=int(row[14]),
            channel=int(row[16]),
            local_timestamp_us=int(row[18]) & 0xFFFFFFFF,
            rx_state=int(row[21]),
            first_word_invalid=int(row[23]),
            csi=csi,
        )
    except (SyntaxError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid CSI_DATA fields: {exc}") from exc


def parse_record(envelope: Envelope) -> CsiFrame | Heartbeat | None:
    raw = envelope.raw
    if raw.startswith("CSI_DATA,"):
        return parse_csi(envelope)
    if raw.startswith("CWSLAB_TIMING_HEARTBEAT ") or raw.startswith("CSI_PROFILE "):
        return Heartbeat(
            source_id=envelope.source_id,
            ingest_wall_time_ns=envelope.ingest_wall_time_ns,
            ingest_monotonic_ns=envelope.ingest_monotonic_ns,
            values=parse_key_values(raw),
        )
    return None
