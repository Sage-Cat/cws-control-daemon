from __future__ import annotations

import argparse
import gzip
import heapq
import json
import socket
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Iterator, TextIO

from cws_control.engine import CommandActuator, ControlEngine, load_controller_config
from cws_control.context import atomic_write, context_loop, sample_network_context
from cws_control.parser import ParseError, parse_envelope
from cws_control.scenarios import run_failure_scenarios
from cws_control.reporting import write_reports
from cws_control.sweep import run_rate_sweep
from cws_control.prpl import serve_prpl_telemetry
from cws_control.method_blocks import run_method_blocks
from cws_control.pipeline import restore_sacm_state
from cws_control.reference import (
    DEFAULT_MAX_REQUEST_BYTES,
    decode_reference_request,
    execute_reference_request,
    serve_reference_socket,
)


def open_text(path: Path, stack: ExitStack) -> TextIO:
    if path.suffix == ".gz" or path.name.endswith(".gz.partial"):
        return stack.enter_context(gzip.open(path, "rt", encoding="utf-8"))
    return stack.enter_context(path.open("r", encoding="utf-8"))


def envelope_stream(stream: TextIO) -> Iterator[tuple[int, str]]:
    for line in stream:
        if not line.strip():
            continue
        try:
            envelope = parse_envelope(line)
        except ParseError:
            continue
        yield envelope.ingest_wall_time_ns, line


def merged_envelopes(paths: list[Path]) -> Iterator[tuple[int, str]]:
    with ExitStack() as stack:
        streams = [envelope_stream(open_text(path, stack)) for path in paths]
        yield from heapq.merge(*streams, key=lambda item: item[0])


def write_decision(stream: TextIO, decision: dict) -> None:
    stream.write(json.dumps(decision, separators=(",", ":"), sort_keys=True) + "\n")
    stream.flush()


def run_replay(args: argparse.Namespace) -> int:
    config = load_controller_config(args.config)
    config["mode"] = "observe"
    engine = ControlEngine(config)
    interval_ns = int(float(config.get("decision_interval_seconds", 2.0)) * 1e9)
    next_decision: int | None = None
    with args.output.open("w", encoding="utf-8") as output:
        for wall_ns, line in merged_envelopes(args.input):
            engine.ingest(line)
            if next_decision is None:
                next_decision = wall_ns + interval_ns
            while wall_ns >= next_decision:
                write_decision(output, engine.decide(next_decision))
                next_decision += interval_ns
        if next_decision is not None:
            write_decision(output, engine.decide(next_decision))
    return 0


def run_serve(args: argparse.Namespace) -> int:
    config = load_controller_config(args.config)
    actuator = CommandActuator(config.get("actuators", {})) if config.get("mode") == "apply" else None
    engine = ControlEngine(config, actuator)
    host = engine.listen_host
    port = int(config.get("listen_port", 8765))
    interval = float(config.get("decision_interval_seconds", 2.0))
    destination = args.output
    destination.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration if args.duration else None
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    sock.settimeout(min(0.5, interval))
    next_decision = time.monotonic() + interval
    with destination.open("a", encoding="utf-8") as output, sock:
        while deadline is None or time.monotonic() < deadline:
            try:
                payload, _address = sock.recvfrom(65535)
                engine.ingest(payload)
            except socket.timeout:
                pass
            now = time.monotonic()
            if now >= next_decision:
                write_decision(output, engine.decide())
                next_decision += interval
    return 0


def run_sample_context(args: argparse.Namespace) -> int:
    value = sample_network_context(args.interface, args.gateway)
    if args.output:
        atomic_write(args.output, value)
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


def run_context_daemon(args: argparse.Namespace) -> int:
    context_loop(args.current, args.history, args.interval, args.interface, args.gateway)
    return 0


def run_prpl_receiver(args: argparse.Namespace) -> int:
    serve_prpl_telemetry(args.host, args.port, args.current, args.history, args.token_file)
    return 0


def run_failure_simulation(args: argparse.Namespace) -> int:
    report = run_failure_scenarios()
    if args.output:
        atomic_write(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


def run_summarize(args: argparse.Namespace) -> int:
    summary = write_reports(args.input, args.output, args.markdown, args.events)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def run_sweep(args: argparse.Namespace) -> int:
    config = load_controller_config(args.config)
    rates = [int(value) for value in args.rates.split(",")]
    report = run_rate_sweep(config, rates, args.phase_seconds, args.output)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "complete" else 1


def run_blocks(args: argparse.Namespace) -> int:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    report = run_method_blocks(config, args.output_dir)
    print(json.dumps({"status": report["status"], "phases": len(report["phases"])}, indent=2))
    return 0 if report["status"] == "complete" else 1


def run_reference(args: argparse.Namespace) -> int:
    """Call the sole pure M1--M4 implementation and persist only derived output."""

    destinations = [args.output]
    if args.state_output is not None:
        destinations.append(args.state_output)
    inputs = [args.input]
    if args.state_input is not None:
        inputs.append(args.state_input)
    resolved_destinations = [path.resolve() for path in destinations]
    if len(set(resolved_destinations)) != len(resolved_destinations) or any(
        destination == source.resolve()
        for destination in resolved_destinations
        for source in inputs
    ):
        raise ValueError("reference pipeline input and output paths must be distinct")
    payload = decode_reference_request(args.input.read_bytes())
    state = None
    if args.state_input is not None:
        state = decode_reference_request(args.state_input.read_bytes())
    output, next_state = execute_reference_request(
        payload,
        external_state=state,
        external_state_supplied=args.state_input is not None,
    )
    if args.state_output is not None and next_state is None:
        raise ValueError("reference pipeline produced no SACM state to persist")
    atomic_write(args.output, output)
    if args.state_output is not None and next_state is not None:
        atomic_write(args.state_output, restore_sacm_state(next_state))
    return 0


def run_reference_serve(args: argparse.Namespace) -> int:
    """Expose only the exact reference executor on a protected local socket."""

    serve_reference_socket(
        args.socket,
        state_path=args.state,
        history_path=args.history,
        max_request_bytes=args.max_request_bytes,
        max_requests=args.max_requests,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cooperative Wi-Fi sensing control daemon")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate-config")
    validate.add_argument("--config", type=Path, required=True)
    validate.set_defaults(handler=lambda args: (ControlEngine(load_controller_config(args.config)), 0)[1])
    replay = subparsers.add_parser("replay")
    replay.add_argument("--config", type=Path, required=True)
    replay.add_argument("--input", type=Path, action="append", required=True)
    replay.add_argument("--output", type=Path, required=True)
    replay.set_defaults(handler=run_replay)
    serve = subparsers.add_parser("serve")
    serve.add_argument("--config", type=Path, required=True)
    serve.add_argument("--output", type=Path, required=True)
    serve.add_argument("--duration", type=float, help="optional validation duration in seconds")
    serve.set_defaults(handler=run_serve)
    context = subparsers.add_parser("sample-context")
    context.add_argument("--interface")
    context.add_argument("--gateway")
    context.add_argument("--output", type=Path)
    context.set_defaults(handler=run_sample_context)
    context_daemon = subparsers.add_parser("context-daemon")
    context_daemon.add_argument("--interface")
    context_daemon.add_argument("--gateway")
    context_daemon.add_argument("--current", type=Path, required=True)
    context_daemon.add_argument("--history", type=Path, required=True)
    context_daemon.add_argument("--interval", type=float, default=10.0)
    context_daemon.set_defaults(handler=run_context_daemon)
    prpl = subparsers.add_parser("prpl-receiver")
    prpl.add_argument("--host", default="127.0.0.1")
    prpl.add_argument("--port", type=int, default=8770)
    prpl.add_argument("--current", type=Path, required=True)
    prpl.add_argument("--history", type=Path, required=True)
    prpl.add_argument("--token-file", type=Path, required=True)
    prpl.set_defaults(handler=run_prpl_receiver)
    failures = subparsers.add_parser("simulate-failures")
    failures.add_argument("--output", type=Path)
    failures.set_defaults(handler=run_failure_simulation)
    summarize = subparsers.add_parser("summarize")
    summarize.add_argument("--input", type=Path, required=True)
    summarize.add_argument("--output", type=Path, required=True)
    summarize.add_argument("--markdown", type=Path)
    summarize.add_argument(
        "--events", type=Path, action="append", default=[],
        help="collector events.ndjson path; repeat for each node",
    )
    summarize.set_defaults(handler=run_summarize)
    sweep = subparsers.add_parser("rate-sweep")
    sweep.add_argument("--config", type=Path, required=True)
    sweep.add_argument("--rates", default="10,20,40")
    sweep.add_argument("--phase-seconds", type=float, default=300.0)
    sweep.add_argument("--output", type=Path, required=True)
    sweep.set_defaults(handler=run_sweep)
    blocks = subparsers.add_parser("method-blocks")
    blocks.add_argument("--config", type=Path, required=True)
    blocks.add_argument("--output-dir", type=Path, required=True)
    blocks.set_defaults(handler=run_blocks)
    reference = subparsers.add_parser(
        "reference-pipeline",
        help="run the pure M1--M4 reference path from one reviewed JSON input",
    )
    reference.add_argument("--input", type=Path, required=True)
    reference.add_argument("--output", type=Path, required=True)
    reference.add_argument("--state-input", type=Path)
    reference.add_argument("--state-output", type=Path)
    reference.set_defaults(handler=run_reference)
    reference_serve = subparsers.add_parser(
        "reference-serve",
        help="serve the exact pure M1--M4 reference path on one local Unix socket",
    )
    reference_serve.add_argument("--socket", type=Path, required=True)
    reference_serve.add_argument("--state", type=Path)
    reference_serve.add_argument("--history", type=Path)
    reference_serve.add_argument(
        "--max-request-bytes",
        type=int,
        default=DEFAULT_MAX_REQUEST_BYTES,
    )
    reference_serve.add_argument("--max-requests", type=int)
    reference_serve.set_defaults(handler=run_reference_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
