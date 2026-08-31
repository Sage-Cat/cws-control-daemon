from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def run(argv: list[str], timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)


def command(name: str) -> str:
    """Resolve administrative tools even under a minimal service/SSH PATH."""
    search_path = os.pathsep.join(filter(None, (os.environ.get("PATH"), "/usr/sbin", "/sbin")))
    return shutil.which(name, path=search_path) or name


def default_route() -> tuple[str | None, str | None, str | None]:
    try:
        completed = run([command("ip"), "-json", "route", "show", "default"])
        rows = json.loads(completed.stdout) if completed.returncode == 0 else []
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError):
        return None, None, "default-route-unavailable"
    if not rows:
        return None, None, "default-route-unavailable"
    row = rows[0]
    return row.get("dev"), row.get("gateway"), None


def survey_busy_percent(interface: str) -> tuple[float | None, str | None]:
    try:
        completed = run([command("iw"), "dev", interface, "survey", "dump"])
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"survey-unavailable:{exc}"
    if completed.returncode != 0:
        return None, "survey-unsupported"
    blocks = re.split(r"(?=Survey data from)", completed.stdout)
    block = next((candidate for candidate in blocks if "[in use]" in candidate), "")
    active = re.search(r"channel active time:\s*([0-9]+)\s*ms", block)
    busy = re.search(r"channel busy time:\s*([0-9]+)\s*ms", block)
    if not active or not busy or int(active.group(1)) <= 0:
        return None, "survey-counters-missing"
    return 100.0 * int(busy.group(1)) / int(active.group(1)), None


def ping_context(gateway: str) -> tuple[float | None, float | None, str | None]:
    try:
        completed = run([command("ping"), "-n", "-c", "3", "-W", "1", gateway], timeout=5.0)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, None, f"ping-unavailable:{exc}"
    loss = re.search(r"([0-9.]+)% packet loss", completed.stdout)
    rtt = re.search(r"=\s*[0-9.]+/([0-9.]+)/", completed.stdout)
    if not loss:
        return None, None, "ping-unparseable"
    return (float(rtt.group(1)) if rtt else None), float(loss.group(1)), None


def classify_load(busy_percent: float | None, latency_ms: float | None, loss_percent: float | None) -> str:
    level = 0
    if busy_percent is not None:
        level = max(level, 0 if busy_percent < 25 else 1 if busy_percent < 50 else 2 if busy_percent < 75 else 3)
    if latency_ms is not None:
        level = max(level, 0 if latency_ms < 20 else 1 if latency_ms < 50 else 2 if latency_ms < 100 else 3)
    if loss_percent is not None:
        level = max(level, 0 if loss_percent == 0 else 1 if loss_percent < 2 else 2 if loss_percent < 10 else 3)
    return ("light", "moderate", "elevated", "high")[level]


def sample_network_context(interface: str | None = None, gateway: str | None = None) -> dict[str, Any]:
    problems: list[str] = []
    if interface is None or gateway is None:
        route_interface, route_gateway, route_problem = default_route()
        interface = interface or route_interface
        gateway = gateway or route_gateway
        if route_problem:
            problems.append(route_problem)
    busy = None
    if interface:
        busy, problem = survey_busy_percent(interface)
        if problem:
            problems.append(problem)
    latency = loss = None
    if gateway:
        latency, loss, problem = ping_context(gateway)
        if problem:
            problems.append(problem)
    load = classify_load(busy, latency, loss)
    return {
        "schema_version": 1,
        "sampled_at": utc_now(),
        "sampled_wall_time_ns": time.time_ns(),
        "interface": interface,
        "gateway": gateway,
        "channel_busy_percent": busy,
        "gateway_latency_mean_ms": latency,
        "gateway_packet_loss_percent": loss,
        "load": load,
        "quality": "measured" if any(value is not None for value in (busy, latency, loss)) else "fallback",
        "problems": problems,
        "classification": "deployment-calibrated-thresholds-v1",
    }


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def append_history(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n")
        stream.flush()


def context_loop(current: Path, history: Path, interval: float, interface: str | None, gateway: str | None) -> None:
    while True:
        value = sample_network_context(interface, gateway)
        atomic_write(current, value)
        append_history(history, value)
        time.sleep(interval)
