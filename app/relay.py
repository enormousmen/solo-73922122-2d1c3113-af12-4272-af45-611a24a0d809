"""Core dual-path merge relay.

Business rules (see README for the full spec):

* Two UDP paths carry the same programme. The first packet received anchors
  the stream: its sequence number becomes the first one to forward.
* RTP sequence numbers are 16-bit and may wrap around. All comparisons go
  through modular arithmetic.
* Duplicate packets (same sequence from the other path, or a retransmit) are
  dropped -- the first copy wins.
* Only packets whose forward distance from the pending sequence is within
  ``window`` positions (default 64) are accepted; anything further ahead is
  dropped as it would require buffering beyond the allowed window.
* When a gap appears, the relay waits ``gap_timeout_ms`` (80 ms). The wait is
  anchored to the arrival of the *first later packet*; later arrivals never
  extend it. If both paths stay silent, the timer still fires, the missing
  number is skipped and forwarding continues.
* Skipped sequence numbers are retained (newest first, last 64) and exposed
  through FastAPI.

Optional delay-broadcast mode (enabled at startup, see ``DELAY_ENABLED``):
merged packets are not sent immediately. They enter :class:`~app.delay_queue.
DelayQueue`, receive a monotonic, never-reused queue id independent of the
wrapping RTP sequence number, and are sent in order once each has aged the
configured delay (default 8 s). The operator can list pending ids with their
RTP sequence numbers and retract a closed id interval; a retraction is
all-or-nothing and retracted packets never reach the playout machine.

The engine is entirely single-threaded: every method runs inside one asyncio
event loop, so plain containers need no locking.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from .delay_queue import DelayQueue, RetractionError
from .rtp import is_rtp_packet, seq_delta, sequence_number


@dataclass
class RelayStats:
    received: int = 0            # valid RTP packets offered to the engine
    duplicates: int = 0          # packets whose seq was already buffered/sent
    out_of_window: int = 0       # packets beyond the 64-position window
    invalid: int = 0             # non-RTP datagrams
    forwarded: int = 0           # packets handed to the UDP send socket
    skipped: int = 0             # RTP numbers given up on after the gap wait
    gaps_recovered: int = 0      # gaps closed by a copy from either path
    gaps_timed_out: int = 0      # gaps that ended in a skip
    send_errors: int = 0
    # Delay-broadcast bookkeeping. Operator retractions are deliberately NOT
    # counted as ``skipped``: skips are missing upstream packets, retractions
    # are explicit operator actions on packets that were fully received.
    retracted: int = 0           # packets pulled by the operator before send
    retraction_rejections: int = 0  # retraction requests refused wholesale
    path_received: dict = field(default_factory=lambda: {"A": 0, "B": 0})


class RelayEngine:
    def __init__(self, settings, forward_transport: asyncio.DatagramTransport,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._settings = settings
        self._transport = forward_transport
        self._loop = loop or asyncio.get_event_loop()
        self._modulus = settings.seq_modulus
        self._window = settings.window
        self._timeout = settings.gap_timeout_ms / 1000.0

        # seq -> (raw packet, monotonic arrival time)
        self._buffer: dict[int, tuple[bytes, float]] = {}
        self._next_seq: Optional[int] = None
        self._gap_deadline: Optional[float] = None
        self._gap_timer: Optional[asyncio.TimerHandle] = None
        self._gap_active = False  # a gap wait is currently in progress

        # Bounded deque keeps only the most recent 64 skipped numbers,
        # newest first.
        self._skipped: deque[int] = deque(maxlen=64)
        self.stats = RelayStats()

        # Delay-broadcast mode is opt-in at startup and immutable afterwards.
        self._delay_enabled = settings.delay_enabled
        self._delay_queue: Optional[DelayQueue] = None
        if self._delay_enabled:
            self._delay_queue = DelayQueue(
                settings.delay_seconds, self._egress_send, loop=self._loop)

    # ------------------------------------------------------------------ API

    def handle_packet(self, data: bytes, path: str) -> None:
        """Entry point used by both UDP ingress transports."""
        if not is_rtp_packet(data):
            self.stats.invalid += 1
            return
        seq = sequence_number(data)
        self.stats.received += 1
        self.stats.path_received[path] = self.stats.path_received.get(path, 0) + 1

        # First packet anchors the stream. It leaves the merge stage via
        # _deliver: immediate egress in normal mode, held-and-numbered in
        # delay mode. Nothing is cached before this anchor.
        if self._next_seq is None:
            self._next_seq = seq
            self._deliver(seq, data)
            self._pump()
            return

        distance = seq_delta(seq, self._next_seq, self._modulus)
        if distance < 0:
            # Already sent, or a stale copy behind the pending sequence.
            self.stats.duplicates += 1
            return
        if distance >= self._window:
            # Further ahead than the allowed 64 positions.
            self.stats.out_of_window += 1
            return
        if seq in self._buffer:
            self.stats.duplicates += 1
            return

        self._buffer[seq] = (data, self._loop.time())
        self._pump()

    def recent_skipped(self) -> list[int]:
        return list(self._skipped)

    # ------------------------------------------------- delay-broadcast API

    @property
    def delay_enabled(self) -> bool:
        return self._delay_enabled

    def pending_packets(self) -> list[dict]:
        """Queue-id -> RTP-seq view of packets not yet sent/retracted.

        Empty (and cheap) when delay-broadcast mode is disabled.
        """
        if self._delay_queue is None:
            return []
        return self._delay_queue.pending()

    def retract_range(self, start_id: int, end_id: int) -> dict:
        """Retract a closed queue-id interval; all-or-nothing semantics.

        Re-raises :class:`ValueError` for malformed intervals and
        :class:`RetractionError` when any id is not currently pending.
        """
        if self._delay_queue is None:
            raise RuntimeError("delay-broadcast mode is disabled")
        try:
            result = self._delay_queue.retract_range(start_id, end_id)
        except (ValueError, RetractionError):
            self.stats.retraction_rejections += 1
            raise
        self.stats.retracted = self._delay_queue.retracted
        return result

    def status(self) -> dict:
        snap = {
            "next_seq": self._next_seq,
            "buffered": len(self._buffer),
            "gap_wait_active": self._gap_active,
            "gap_deadline_in_ms": (
                round((self._gap_deadline - self._loop.time()) * 1000, 1)
                if self._gap_deadline is not None else None
            ),
            "recent_skipped": self.recent_skipped(),
            "stats": self.stats.__dict__.copy(),
        }
        if self._delay_queue is not None:
            # RelayStats already mirrors the egress counters via _egress_send;
            # the delay snapshot adds queue-specific scheduling detail.
            snap["delay"] = self._delay_queue.snapshot()
            snap["pending"] = self._delay_queue.pending()
        else:
            snap["delay"] = {"enabled": False}
        return snap

    async def aclose(self) -> None:
        self._cancel_timer()
        self._gap_active = False
        if self._delay_queue is not None:
            await self._delay_queue.aclose()

    # -------------------------------------------------------------- internals

    def _egress_send(self, data: bytes) -> None:
        """Hand a raw datagram to the forward socket; shared egress path.

        This is the single place a packet is handed to UDP, so the
        ``forwarded``/``send_errors`` counters mean exactly the same thing in
        immediate and delay modes. The delay queue calls this from its timer.
        """
        try:
            self._transport.sendto(data, (self._settings.forward_host,
                                          self._settings.forward_port))
        except Exception:  # pragma: no cover - defensive, never kill the loop
            self.stats.send_errors += 1
        finally:
            self.stats.forwarded += 1

    def _deliver(self, seq: int, data: bytes) -> None:
        """A merged packet leaves the merge stage.

        In delay mode it enters the delay queue (held, queue-id numbered);
        otherwise it is handed straight to the UDP socket. Either way the
        RTP pending sequence advances immediately -- merge order is already
        settled at this point.
        """
        if self._delay_queue is not None:
            self._delay_queue.enqueue(data, seq)
        else:
            self._egress_send(data)
        self._next_seq = (seq + 1) % self._modulus

    def _pump(self) -> None:
        """Forward everything available from the pending sequence upward,
        then (re)arm the gap wait when the head is missing."""
        while self._next_seq in self._buffer:
            if self._gap_active:
                # The missing head arrived before its deadline: the other
                # path (or a retransmit) filled the gap.
                self.stats.gaps_recovered += 1
                self._gap_active = False
            data, _ = self._buffer.pop(self._next_seq)
            self._deliver(self._next_seq, data)

        if self._buffer:
            # Head is missing and later packets exist. Anchor the wait to the
            # earliest arrival; later packets never move the deadline.
            earliest = min(arrival for _, arrival in self._buffer.values())
            self._reschedule(earliest + self._timeout)
            self._gap_active = True
        else:
            # Nothing pending. This also cancels a timer whose gap was just
            # resolved, so the callback cannot fire and skip a live number.
            self._cancel_timer()
            self._gap_active = False

    def _reschedule(self, deadline: float) -> None:
        if self._gap_timer is not None:
            self._gap_timer.cancel()
        self._gap_deadline = deadline
        delay = max(0.0, deadline - self._loop.time())
        self._gap_timer = self._loop.call_later(delay, self._on_gap_timeout)

    def _on_gap_timeout(self) -> None:
        self._gap_timer = None
        self._gap_deadline = None

        if self._next_seq is not None and self._next_seq in self._buffer:
            # Filled at the last moment before the callback ran.
            self._pump()
            return

        self._gap_active = False
        self.stats.gaps_timed_out += 1
        # Skip every contiguous missing number up to the first buffered one.
        # With no buffered packets at all (both paths silent) this skips the
        # single missing number and the engine goes back to idle, ready to
        # keep pumping when the next packet arrives.
        while self._next_seq not in self._buffer:
            self._skipped.appendleft(self._next_seq)
            self.stats.skipped += 1
            self._next_seq = (self._next_seq + 1) % self._modulus
            if not self._buffer:
                break
        self._pump()

    def _cancel_timer(self) -> None:
        if self._gap_timer is not None:
            self._gap_timer.cancel()
            self._gap_timer = None
        self._gap_deadline = None
