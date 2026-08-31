"""Safe local boundaries around the exact pure M1--M4 reference pipeline."""
from __future__ import annotations

import json
import os
import socket
import stat
from pathlib import Path
from typing import Any, Callable, Mapping

from cws_control.context import atomic_write
from cws_control.contracts import ContractError, canonical_sha256, contract
from cws_control.pipeline import restore_sacm_state, run_reference_pipeline


REFERENCE_RUN_V1 = "cws-reference-pipeline-run/1"
REFERENCE_SERVICE_ERROR_V1 = "cws-reference-service-error/1"
DEFAULT_MAX_REQUEST_BYTES = 1_048_576
MAX_MAX_REQUEST_BYTES = 16_777_216
_REQUIRED_REQUEST_FIELDS = frozenset({"m1", "m2", "m3"})
_OPTIONAL_REQUEST_FIELDS = frozenset({"m4_candidate", "parameters", "state"})


class ReferenceRequestError(ValueError):
    """A typed, data-free reason for rejecting a reference request."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _reject_constant(_value: str) -> None:
    raise ReferenceRequestError("invalid-json-number")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ReferenceRequestError("duplicate-json-key")
        value[key] = item
    return value


def decode_reference_request(encoded: bytes) -> dict[str, Any]:
    """Decode exactly one finite JSON object without retaining input bytes."""

    try:
        value = json.loads(
            encoded,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except ReferenceRequestError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ReferenceRequestError("invalid-json") from exc
    if not isinstance(value, dict):
        raise ReferenceRequestError("request-not-object")
    return value


def _validated_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ReferenceRequestError("request-not-object")
    value = dict(payload)
    missing = _REQUIRED_REQUEST_FIELDS - value.keys()
    if missing:
        raise ReferenceRequestError(f"missing-{sorted(missing)[0]}")
    unexpected = value.keys() - (_REQUIRED_REQUEST_FIELDS | _OPTIONAL_REQUEST_FIELDS)
    if unexpected:
        raise ReferenceRequestError("unexpected-request-field")
    for field in _REQUIRED_REQUEST_FIELDS:
        if not isinstance(value[field], Mapping):
            raise ReferenceRequestError(f"invalid-{field}")
    for field in ("m4_candidate", "parameters", "state"):
        if field in value and value[field] is not None and not isinstance(value[field], Mapping):
            raise ReferenceRequestError(f"invalid-{field.replace('_', '-')}")
    return value


def execute_reference_request(
    payload: Mapping[str, Any],
    *,
    external_state: Mapping[str, Any] | None = None,
    external_state_supplied: bool = False,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Validate one request, call the sole pure core, and derive one run record."""

    value = _validated_request(payload)
    if "state" in value and external_state_supplied:
        raise ReferenceRequestError("duplicate-state-input")
    raw_state = external_state if external_state_supplied else value.get("state")
    try:
        input_sha256 = canonical_sha256(value)
        state = restore_sacm_state(raw_state) if raw_state is not None else None
        pipeline_result = run_reference_pipeline(
            m1=value["m1"],
            m2=value["m2"],
            m3=value["m3"],
            m4_candidate=value.get("m4_candidate"),
            state=state,
            parameters=value.get("parameters"),
        )
    except ReferenceRequestError:
        raise
    except ContractError as exc:
        raise ReferenceRequestError(str(exc)) from exc
    except (TypeError, ValueError, OverflowError) as exc:
        raise ReferenceRequestError("invalid-request-value") from exc
    next_state = pipeline_result.get("state") or state
    output = contract(
        REFERENCE_RUN_V1,
        {
            "input_sha256": input_sha256,
            "status": pipeline_result["status"],
            "failed_stage": pipeline_result.get("failed_stage"),
            "ledger": pipeline_result["ledger"],
            "result": pipeline_result["result"],
            "state_sha256": next_state.get("sha256") if next_state else None,
        },
    )
    return output, next_state


def reference_error(reason: str) -> dict[str, Any]:
    return contract(
        REFERENCE_SERVICE_ERROR_V1,
        {"status": "error", "reason": reason},
    )


def _append_derived_output(path: Path, value: Mapping[str, Any]) -> None:
    """Append one compact derived record without ever accepting request fields."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        dict(value),
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("derived-history-write-failed")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_request(connection: socket.socket, maximum: int) -> bytes:
    payload = bytearray()
    while True:
        chunk = connection.recv(min(65_536, maximum + 1 - len(payload)))
        if not chunk:
            return bytes(payload)
        payload.extend(chunk)
        if len(payload) > maximum:
            raise ReferenceRequestError("request-too-large")


def _same_created_socket(path: Path, identity: tuple[int, int] | None) -> bool:
    if identity is None:
        return False
    try:
        current = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISSOCK(current.st_mode) and (current.st_dev, current.st_ino) == identity


def serve_reference_socket(
    socket_path: Path,
    *,
    state_path: Path | None = None,
    history_path: Path | None = None,
    max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
    max_requests: int | None = None,
    ready: Callable[[], None] | None = None,
) -> None:
    """Serve one request per local connection, sequentially and fail closed."""

    if (
        isinstance(max_request_bytes, bool)
        or not isinstance(max_request_bytes, int)
        or not 1 <= max_request_bytes <= MAX_MAX_REQUEST_BYTES
    ):
        raise ValueError("invalid-max-request-bytes")
    if max_requests is not None and (
        isinstance(max_requests, bool) or not isinstance(max_requests, int) or max_requests < 1
    ):
        raise ValueError("invalid-max-requests")
    if not socket_path.parent.is_dir():
        raise ValueError("socket-parent-missing")
    if os.path.lexists(socket_path):
        raise ValueError("socket-path-exists")
    destinations = [path.resolve() for path in (state_path, history_path) if path is not None]
    if len(destinations) != len(set(destinations)) or socket_path.resolve() in destinations:
        raise ValueError("reference-service-paths-must-be-distinct")

    current_state: dict[str, Any] | None = None
    if state_path is not None and state_path.exists():
        try:
            stored = decode_reference_request(state_path.read_bytes())
            current_state = restore_sacm_state(stored)
        except ReferenceRequestError as exc:
            raise ValueError("invalid-persistent-state") from exc
        except (ContractError, OSError, TypeError, ValueError) as exc:
            raise ValueError("invalid-persistent-state") from exc

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    identity: tuple[int, int] | None = None
    try:
        old_umask = os.umask(0o177)
        try:
            listener.bind(str(socket_path))
        finally:
            os.umask(old_umask)
        created = socket_path.lstat()
        identity = (created.st_dev, created.st_ino)
        os.chmod(socket_path, 0o600)
        listener.listen(1)
        if ready is not None:
            ready()
        handled = 0
        while max_requests is None or handled < max_requests:
            connection, _address = listener.accept()
            handled += 1
            with connection:
                try:
                    payload = decode_reference_request(
                        _read_request(connection, max_request_bytes)
                    )
                    response, next_state = execute_reference_request(
                        payload,
                        external_state=current_state,
                        external_state_supplied=state_path is not None or current_state is not None,
                    )
                    if state_path is not None and next_state is not None:
                        atomic_write(state_path, restore_sacm_state(next_state))
                    if history_path is not None:
                        _append_derived_output(history_path, response)
                    current_state = next_state
                except (BrokenPipeError, ConnectionResetError):
                    continue
                except ReferenceRequestError as exc:
                    response = reference_error(exc.reason)
                encoded = json.dumps(
                    response,
                    separators=(",", ":"),
                    sort_keys=True,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8") + b"\n"
                try:
                    connection.sendall(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    pass
    finally:
        listener.close()
        if _same_created_socket(socket_path, identity):
            socket_path.unlink()
