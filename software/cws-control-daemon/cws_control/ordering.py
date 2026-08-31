"""Bounded, deterministic source-record ordering before scientific parsing."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from cws_control.models import Envelope


@dataclass
class OrderingState:
    """Reorder one source without clocks, timers, or unbounded history."""

    window: int
    event_limit: int
    context_history_limit: int
    session_id: str | None = None
    connection_epoch: int | None = None
    expected_sequence: int | None = None
    buffer: dict[int, Envelope] = field(default_factory=dict)
    delivered: deque[int] = field(default_factory=deque)
    delivered_set: set[int] = field(default_factory=set)
    retired_contexts: deque[tuple[str, int]] = field(default_factory=deque)
    retired_context_set: set[tuple[str, int]] = field(default_factory=set)
    recent_events: deque[dict[str, Any]] = field(default_factory=deque)
    counts: dict[str, int] = field(
        default_factory=lambda: {
            "gaps": 0,
            "missing_records": 0,
            "duplicates": 0,
            "late": 0,
            "resets": 0,
            "reconnects": 0,
            "buffered": 0,
            "flushed_buffer_records": 0,
        }
    )

    def __post_init__(self) -> None:
        if not 1 <= self.window <= 4096:
            raise ValueError("ordering window must be between 1 and 4096 records")
        if not 1 <= self.event_limit <= 4096:
            raise ValueError("ordering event limit must be between 1 and 4096")
        if not 1 <= self.context_history_limit <= 4096:
            raise ValueError("ordering context history limit must be between 1 and 4096")
        self.delivered = deque(maxlen=self.window * 2 + 1)
        self.retired_contexts = deque(maxlen=self.context_history_limit)
        self.recent_events = deque(maxlen=self.event_limit)

    @property
    def context(self) -> tuple[str, int] | None:
        if self.session_id is None or self.connection_epoch is None:
            return None
        return self.session_id, self.connection_epoch

    def _event(self, event: str, envelope: Envelope, **facts: Any) -> None:
        self.recent_events.append(
            {
                "event": event,
                "session_id": envelope.session_id,
                "connection_epoch": envelope.connection_epoch,
                "source_sequence": envelope.source_sequence,
                **facts,
            }
        )

    def _remember_retired(self, context: tuple[str, int] | None) -> None:
        if context is None or context in self.retired_context_set:
            return
        if len(self.retired_contexts) == self.retired_contexts.maxlen:
            removed = self.retired_contexts.popleft()
            self.retired_context_set.discard(removed)
        self.retired_contexts.append(context)
        self.retired_context_set.add(context)

    def _remember_delivered(self, sequence: int) -> None:
        if len(self.delivered) == self.delivered.maxlen:
            removed = self.delivered.popleft()
            self.delivered_set.discard(removed)
        self.delivered.append(sequence)
        self.delivered_set.add(sequence)

    def _start_context(self, envelope: Envelope, event: str | None) -> bool:
        previous = self.context
        discarded = len(self.buffer)
        self._remember_retired(previous)
        self.buffer.clear()
        if discarded:
            self.counts["flushed_buffer_records"] += discarded
        self.delivered.clear()
        self.delivered_set.clear()
        self.session_id = envelope.session_id
        self.connection_epoch = envelope.connection_epoch
        self.expected_sequence = envelope.source_sequence
        if event is not None:
            counter = "resets" if event == "reset" else "reconnects"
            self.counts[counter] += 1
            self._event(event, envelope, discarded_buffer_records=discarded)
        return event is not None

    def _classify_context(self, envelope: Envelope) -> tuple[bool, bool]:
        """Return ``(accepted, reset_timing_before_delivery)``."""

        incoming = (envelope.session_id, envelope.connection_epoch)
        current = self.context
        if current is None:
            return True, self._start_context(envelope, None)
        if incoming == current:
            return True, False
        if incoming in self.retired_context_set or (
            envelope.session_id == self.session_id
            and self.connection_epoch is not None
            and envelope.connection_epoch < self.connection_epoch
        ):
            self.counts["late"] += 1
            self._event("stale-context", envelope)
            return False, False
        event = "reconnect" if envelope.session_id == self.session_id else "reset"
        return True, self._start_context(envelope, event)

    def _drain(self) -> list[Envelope]:
        ordered: list[Envelope] = []
        while self.expected_sequence in self.buffer:
            assert self.expected_sequence is not None
            sequence = self.expected_sequence
            ordered.append(self.buffer.pop(sequence))
            self._remember_delivered(sequence)
            self.expected_sequence += 1
        return ordered

    def admit(self, envelope: Envelope) -> tuple[list[Envelope], bool, bool]:
        """Admit a record.

        Returns ordered records, whether the input was accepted, and whether
        timing must be reset immediately before the returned records.
        """

        accepted, reset_timing = self._classify_context(envelope)
        if not accepted:
            return [], False, False
        assert self.expected_sequence is not None
        sequence = envelope.source_sequence
        if sequence < self.expected_sequence:
            if sequence in self.delivered_set:
                self.counts["duplicates"] += 1
                self._event("duplicate", envelope)
            else:
                self.counts["late"] += 1
                self._event("late", envelope, expected_sequence=self.expected_sequence)
            return [], False, False
        if sequence in self.buffer:
            self.counts["duplicates"] += 1
            self._event("duplicate", envelope)
            return [], False, False
        self.buffer[sequence] = envelope
        if sequence > self.expected_sequence:
            self.counts["buffered"] += 1
            self._event("buffered", envelope, expected_sequence=self.expected_sequence)

        distance = sequence - self.expected_sequence
        exhausted = len(self.buffer) >= self.window or distance > self.window
        if self.expected_sequence not in self.buffer and exhausted:
            next_sequence = min(self.buffer)
            missing = next_sequence - self.expected_sequence
            self.counts["gaps"] += 1
            self.counts["missing_records"] += missing
            self._event(
                "gap",
                self.buffer[next_sequence],
                missing_start=self.expected_sequence,
                missing_end=next_sequence - 1,
                missing_records=missing,
            )
            self.expected_sequence = next_sequence
            reset_timing = True
        return self._drain(), True, reset_timing

    def ledger(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "connection_epoch": self.connection_epoch,
            "next_expected_sequence": self.expected_sequence,
            "counts": dict(self.counts),
            "buffered_current": len(self.buffer),
            "buffered_sequences": sorted(self.buffer),
            "recent_events": list(self.recent_events),
        }
