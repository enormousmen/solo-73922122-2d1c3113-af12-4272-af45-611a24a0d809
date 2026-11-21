"""Delay-broadcast queue with operator retraction support.

After the dual-path merge, each original RTP datagram is enqueued here and
handed to the UDP forward socket only once it has spent ``delay_seconds`` in
the queue. Two numbering spaces coexist:

* the 16-bit, wrapping RTP sequence number carried in the packet header;
* the *queue id* (待播编号): a process-local, monotonically increasing integer
  assigned in enqueue order. It never wraps and is never reused, including
  across RTP sequence wrap-arounds, so an operator's retraction request can
  never ambiguously refer to two different packets.

Retraction semantics (driven by the API layer):

* a request names a closed, inclusive queue-id interval;
* the whole interval is validated before anything changes -- if any id was
  already sent, already retracted, or has not been enqueued yet, the request
  is rejected atomically and no packet changes state;
* retracted packets are never handed to the forward socket; packets that stay
  pending keep their original schedule (enqueue time + delay), and the queue
  drains normally even when the upstream goes silent, because scheduling is
  driven solely by event-loop timers.

The queue is single-threaded, like the rest of the engine: every method runs
inside one asyncio event loop, so no locking is needed.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional


class EntryState(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    RETRACTED = "retracted"


# Why a retraction request was refused. The API maps these to HTTP 409.
NOT_ENQUEUED = "not_enqueued"
ALREADY_SENT = "already_sent"
ALREADY_RETRACTED = "already_retracted"


class RetractionError(Exception):
    def __init__(self, reason: str, queue_id: int) -> None:
        super().__init__(f"queue id {queue_id}: {reason}")
        self.reason = reason
        self.queue_id = queue_id


@dataclass
class _Entry:
    queue_id: int
    rtp_seq: int
    data: bytes
    enqueued_at: float       # loop.time() of enqueue
    state: EntryState = EntryState.PENDING


# How many finished (sent/retracted) entries are kept around purely so that
# retraction rejections can report the precise reason instead of "unknown".
HISTORY_LIMIT = 4096


class DelayQueue:
    """Timer-driven hold-and-forward queue with retractable entries."""

    def __init__(self, delay_seconds: float,
                 send: Callable[[bytes], None],
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        if delay_seconds <= 0:
            raise ValueError("delay_seconds must be positive")
        self._delay = float(delay_seconds)
        # Egress callback; the engine owns the actual sendto + stats.
        self._send = send
        self._loop = loop or asyncio.get_event_loop()

        # Monotonic queue-id allocator. Starts at 1 and is never decremented;
        # ids of sent/retracted packets are never reissued.
        self._next_id = 1
        # queue id -> entry (pending entries and recently finished ones)
        self._entries: dict[int, _Entry] = {}
        # Enqueue order; head is always the earliest still-pending entry
        # (finished heads are pruned when the timer fires / entries retire).
        self._order: deque[int] = deque()
        # Bounded terminal history for precise rejection reasons.
        self._history: deque[tuple[int, EntryState]] = deque(maxlen=HISTORY_LIMIT)

        self._timer: Optional[asyncio.TimerHandle] = None
        self._deadline: Optional[float] = None

        self.forwarded = 0       # actually handed to the egress callback
        self.retracted = 0       # successfully retracted packets
        self.send_errors = 0

    # ------------------------------------------------------------------ API

    def enqueue(self, data: bytes, rtp_seq: int) -> int:
        """Assign a fresh queue id and schedule the hold, if needed.

        Returns the allocated queue id. Ids are never reused.
        """
        entry = _Entry(
            queue_id=self._next_id,
            rtp_seq=rtp_seq,
            data=data,
            enqueued_at=self._loop.time(),
        )
        self._next_id += 1
        self._entries[entry.queue_id] = entry
        self._order.append(entry.queue_id)
        # Only the head needs a timer; enqueue is FIFO so an existing timer
        # already covers an earlier, no-less-urgent deadline.
        if self._timer is None:
            self._arm(entry.enqueued_at + self._delay)
        return entry.queue_id

    def pending(self) -> list[dict]:
        """All not-yet-sent, not-yet-retracted entries, queue id ascending."""
        now = self._loop.time()
        out: list[dict] = []
        for qid in self._order:
            entry = self._entries[qid]
            if entry.state is not EntryState.PENDING:
                continue
            out.append({
                "queue_id": entry.queue_id,
                "rtp_seq": entry.rtp_seq,
                "enqueued_at_in_ms": round((now - entry.enqueued_at) * 1000, 1),
                "scheduled_in_ms": max(
                    0.0, round((entry.enqueued_at + self._delay - now) * 1000, 1)),
            })
        return out

    def retract_range(self, start_id: int, end_id: int) -> dict:
        """Retract every packet whose queue id is in the closed interval.

        Atomic: the interval is validated in full before any entry changes.
        Raises :class:`RetractionError` on the first unusable id; raises
        ``ValueError`` for a malformed (non-positive / reversed) interval.
        """
        if not isinstance(start_id, int) or not isinstance(end_id, int):
            raise ValueError("start and end must be integers")
        if start_id < 1 or end_id < 1:
            raise ValueError("queue ids start at 1")
        if start_id > end_id:
            raise ValueError("start must be <= end")

        # Phase 1: validate the entire interval first.
        for qid in range(start_id, end_id + 1):
            entry = self._entries.get(qid)
            if entry is not None:
                if entry.state is EntryState.SENT:
                    raise RetractionError(ALREADY_SENT, qid)
                if entry.state is EntryState.RETRACTED:
                    raise RetractionError(ALREADY_RETRACTED, qid)
                # pending -> acceptable
                continue
            # No live entry: either never enqueued or pruned into history.
            state = self._history_state(qid)
            if state is EntryState.SENT:
                raise RetractionError(ALREADY_SENT, qid)
            if state is EntryState.RETRACTED:
                raise RetractionError(ALREADY_RETRACTED, qid)
            raise RetractionError(NOT_ENQUEUED, qid)

        # Phase 2: all ids are pending -> retract as one batch.
        rtp_seqs: list[int] = []
        for qid in range(start_id, end_id + 1):
            entry = self._entries[qid]
            entry.state = EntryState.RETRACTED
            self.retracted += 1
            rtp_seqs.append(entry.rtp_seq)
        self._prune_heads()
        return {
            "retracted": True,
            "range": {"start": start_id, "end": end_id},
            "count": end_id - start_id + 1,
            "rtp_seqs": rtp_seqs,
        }

    def snapshot(self) -> dict:
        pending = self.pending()
        return {
            "enabled": True,
            "delay_seconds": self._delay,
            "pending_count": len(pending),
            "next_queue_id": self._next_id,
            "oldest_pending_queue_id": pending[0]["queue_id"] if pending else None,
            "deadline_in_ms": (
                round((self._deadline - self._loop.time()) * 1000, 1)
                if self._deadline is not None else None
            ),
            "forwarded": self.forwarded,
            "retracted": self.retracted,
            "send_errors": self.send_errors,
        }

    async def aclose(self) -> None:
        self._cancel_timer()

    # -------------------------------------------------------------- internals

    def _fire(self) -> None:
        """Timer callback: send every entry whose hold has elapsed."""
        self._timer = None
        self._deadline = None
        now = self._loop.time()
        # Retracted/sent heads may have been left behind by an earlier fire;
        # retire them first, then hand every due pending entry to egress.
        while self._order:
            entry = self._entries[self._order[0]]
            if entry.state is EntryState.PENDING and entry.enqueued_at + self._delay > now:
                break  # head not due yet; FIFO means nothing behind it is
            if entry.state is EntryState.PENDING:
                try:
                    self._send(entry.data)
                except Exception:  # pragma: no cover - defensive, keep draining
                    self.send_errors += 1
                # The packet was handed to the UDP socket (or the attempt
                # failed inside it): forwarded counts hand-offs either way,
                # matching the non-delay engine's _deliver semantics.
                entry.state = EntryState.SENT
                self.forwarded += 1
            self._retire_head()

        if self._order:
            head = self._entries[self._order[0]]
            self._arm(head.enqueued_at + self._delay)

    def _retire_head(self) -> None:
        qid = self._order.popleft()
        entry = self._entries.pop(qid)
        self._remember(qid, entry.state)

    def _prune_heads(self) -> None:
        """Drop finished entries sitting at the head and rearm the timer."""
        while self._order:
            entry = self._entries[self._order[0]]
            if entry.state is EntryState.PENDING:
                break
            self._retire_head()
        self._cancel_timer()
        if self._order:
            head = self._entries[self._order[0]]
            self._arm(head.enqueued_at + self._delay)

    def _history_state(self, qid: int) -> Optional[EntryState]:
        # deque is small and bounded; a linear scan is fine and keeps the
        # memory footprint far below a second full index.
        for hist_id, state in reversed(self._history):
            if hist_id == qid:
                return state
        return None

    def _remember(self, qid: int, state: EntryState) -> None:
        self._history.append((qid, state))

    def _arm(self, deadline: float) -> None:
        self._cancel_timer()
        self._deadline = deadline
        delay = max(0.0, deadline - self._loop.time())
        self._timer = self._loop.call_later(delay, self._fire)

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._deadline = None
