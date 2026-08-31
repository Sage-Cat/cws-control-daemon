from __future__ import annotations

import hmac
import json
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from cws_control.context import append_history, atomic_write, utc_now


NUMBER = r"(-?[0-9]+(?:\.[0-9]+)?)"


def numeric_values(text: str, names: tuple[str, ...]) -> list[float]:
    values: list[float] = []
    for name in names:
        pattern = rf'(?:"{re.escape(name)}"\s*:\s*|\b{re.escape(name)}\s*=\s*){NUMBER}'
        values.extend(float(match) for match in re.findall(pattern, text, flags=re.IGNORECASE))
    return values


def extract_prpl_context(fields: dict[str, str], received_wall_time_ns: int | None = None) -> dict[str, Any]:
    received_wall_time_ns = received_wall_time_ns or time.time_ns()
    radio = fields.get("radio", "")
    stations = fields.get("stations", "")
    qos = fields.get("qos", "")
    mesh = fields.get("mesh", "")
    utilizations = numeric_values(
        radio + "\n" + mesh,
        ("ChannelLoad", "ChannelUtilization", "Utilization"),
    )
    utilization_percent = [value * 100.0 / 255.0 if value > 100.0 else value for value in utilizations]
    noise = numeric_values(radio, ("Noise", "NoiseFloor"))
    associated = numeric_values(stations, ("AssociatedDeviceNumberOfEntries",))
    drops = numeric_values(
        qos + "\n" + radio,
        ("DiscardPackets", "DiscardPacketsSent", "DiscardPacketsReceived", "DropCount"),
    )
    errors = numeric_values(
        qos + "\n" + radio,
        ("ErrorsSent", "ErrorsReceived", "TxErrors", "RxErrors"),
    )
    channel = numeric_values(radio, ("Channel",))
    telemetry_valid = bool(utilizations or noise or associated or channel)
    maximum_utilization = max(utilization_percent) if utilization_percent else None
    associated_total = int(sum(max(0.0, value) for value in associated)) if associated else None
    if not telemetry_valid:
        load, quality = "moderate", "unavailable"
    elif maximum_utilization is None:
        load = "moderate" if associated_total else "light"
        quality = "partial"
    elif maximum_utilization < 25:
        load, quality = "light", "measured"
    elif maximum_utilization < 50:
        load, quality = "moderate", "measured"
    elif maximum_utilization < 75:
        load, quality = "elevated", "measured"
    else:
        load, quality = "high", "measured"
    return {
        "schema_version": 1,
        "source": "prplos-tr181-agent",
        "node_id": fields.get("node_id", "unknown"),
        "agent_version": fields.get("agent_version", "unknown"),
        "sampled_epoch_ms": int(fields["sampled_epoch_ms"]) if fields.get("sampled_epoch_ms", "").isdigit() else None,
        "received_at": utc_now(),
        "sampled_wall_time_ns": received_wall_time_ns,
        "load": load,
        "quality": quality,
        "telemetry_valid": telemetry_valid,
        "radio_channel_utilization_percent_max": maximum_utilization,
        "radio_channel_utilization_percent_values": utilization_percent,
        "radio_noise_dbm_values": noise,
        "radio_channels": [int(value) for value in channel],
        "associated_device_count": associated_total,
        "qos_drop_counter_values": drops,
        "error_counter_values": errors,
        "raw": {
            key: value
            for key, value in fields.items()
            if key in {"board", "radio", "stations", "mesh", "qos"}
        },
        "classification": "prpl-channel-utilization-thresholds-v1",
    }


class PrplTelemetryServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int], current: Path, history: Path, token: str):
        super().__init__(address, PrplTelemetryHandler)
        self.current = current
        self.history = history
        self.token = token


class PrplTelemetryHandler(BaseHTTPRequestHandler):
    server: PrplTelemetryServer

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def send_status(self, code: int, message: str) -> None:
        payload = json.dumps({"status": message}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/telemetry":
            self.send_status(404, "not-found")
            return
        supplied = self.headers.get("X-CWS-Token", "")
        if not hmac.compare_digest(supplied, self.server.token):
            self.send_status(403, "forbidden")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_status(400, "invalid-length")
            return
        if length <= 0 or length > 1_048_576:
            self.send_status(413, "invalid-size")
            return
        body = self.rfile.read(length).decode("utf-8")
        parsed = parse_qs(body, keep_blank_values=True, strict_parsing=True)
        fields = {key: values[-1] for key, values in parsed.items()}
        if not fields.get("node_id"):
            self.send_status(400, "missing-node-id")
            return
        context = extract_prpl_context(fields)
        atomic_write(self.server.current, context)
        append_history(self.server.history, context)
        self.send_status(200, "accepted")


def serve_prpl_telemetry(host: str, port: int, current: Path, history: Path, token_file: Path) -> None:
    token = token_file.read_text(encoding="utf-8").strip()
    if len(token) < 24:
        raise ValueError("prpl telemetry token must contain at least 24 characters")
    server = PrplTelemetryServer((host, port), current, history, token)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
