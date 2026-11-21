"""Unit tests for the delay-broadcast queue and the engine's delay mode."""
from __future__ import annotations

import asyncio
import unittest

from app.config import Settings
from app.delay_queue import (
    ALREADY_RETRACTED,
    ALREADY_SENT,
    NOT_ENQUEUED,
    DelayQueue,
    RetractionError,
)
from app.relay import RelayEngine
from app.rtp import SEQ_MODULUS, build_packet, sequence_number


def make_settings(**overrides) -> Settings:
    base = dict(
        port_a=9100, port_b=9101,
        forward_host="127.0.0.1", forward_port=9999,
        api_host="127.0.0.1", api_port=8099,
        window=64, gap_timeout_ms=40,
        delay_enabled=True, delay_seconds=0.1,
    )
    base.update(overrides)
    return Settings(**base)


class RecordingTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple]] = []

    def sendto(self, data: bytes, addr: tuple) -> None:
        self.sent.append((data, addr))

    def close(self) -> None:
        pass


class DelayQueueTestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.sent: list[bytes] = []
        self.queue = DelayQueue(0.1, self.sent.append)

    async def advance(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def rtp_seqs(self) -> list[int]:
        return [sequence_number(d) for d in self.sent]


class TestEnqueueAndSchedule(DelayQueueTestBase):
    async def test_ids_are_monotonic_and_start_at_one(self) -> None:
        ids = [self.queue.enqueue(build_packet(100 + i), 100 + i) for i in range(5)]
        self.assertEqual(ids, [1, 2, 3, 4, 5])
        self.assertEqual([p["queue_id"] for p in self.queue.pending()], ids)
        self.assertEqual([p["rtp_seq"] for p in self.queue.pending()],
                         list(range(100, 105)))
        await self.queue.aclose()

    async def test_nothing_sent_before_delay_elapses(self) -> None:
        self.queue.enqueue(build_packet(1), 1)
        await self.advance(0.05)
        self.assertEqual(self.rtp_seqs(), [])
        self.assertEqual(self.queue.snapshot()["pending_count"], 1)

    async def test_sent_after_full_delay_in_enqueue_order(self) -> None:
        for i in range(4):
            self.queue.enqueue(build_packet(i), i)
            await self.advance(0.02)  # spaced arrivals, each keeps own schedule
        await self.advance(0.2)
        self.assertEqual(self.rtp_seqs(), [0, 1, 2, 3])
        self.assertEqual(self.queue.forwarded, 4)
        self.assertEqual(self.queue.pending(), [])

    async def test_drains_after_upstream_goes_silent(self) -> None:
        # Enqueue then stop "feeding" entirely: timers alone must drain it.
        for i in range(3):
            self.queue.enqueue(build_packet(i), i)
        # No further enqueue calls and no external wake-ups.
        deadline = asyncio.get_running_loop().time() + 1
        while self.queue.pending() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        self.assertEqual(self.rtp_seqs(), [0, 1, 2])
        self.assertEqual(self.queue.snapshot()["deadline_in_ms"], None)

    async def test_queue_ids_never_reused_after_rtp_wrap(self) -> None:
        # Two packets carrying the same 16-bit RTP seq number one full cycle
        # apart must still get distinct, monotonic queue ids.
        id_first = self.queue.enqueue(build_packet(7), 7)
        id_second = self.queue.enqueue(build_packet(7), 7)
        self.assertNotEqual(id_first, id_second)
        self.assertEqual(id_second, id_first + 1)
        pending = self.queue.pending()
        self.assertEqual([p["rtp_seq"] for p in pending], [7, 7])
        await self.advance(0.2)
        self.assertEqual(self.rtp_seqs(), [7, 7])  # both original packets sent

    async def test_each_packet_keeps_its_own_schedule(self) -> None:
        # First packet waits the full delay even though more arrive later;
        # later packets do not push the first one's deadline.
        self.queue.enqueue(build_packet(0), 0)
        await self.advance(0.06)
        self.queue.enqueue(build_packet(1), 1)
        await self.advance(0.05)  # total ~0.11 s: first packet due now
        self.assertEqual(self.rtp_seqs(), [0])
        # The second packet, enqueued at ~0.06 s, must wait until ~0.16 s.
        await self.advance(0.06)
        self.assertEqual(self.rtp_seqs(), [0, 1])


class TestRetraction(DelayQueueTestBase):
    async def test_retract_pending_interval_succeeds_and_never_sends(self) -> None:
        ids = [self.queue.enqueue(build_packet(i), i) for i in range(6)]
        result = self.queue.retract_range(2, 4)
        self.assertTrue(result["retracted"])
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["rtp_seqs"], [1, 2, 3])
        self.assertEqual(self.queue.retracted, 3)
        await self.advance(0.25)
        # Retracted packets are absent; the rest still go out in RTP order.
        self.assertEqual(self.rtp_seqs(), [0, 4, 5])
        self.assertEqual(self.queue.forwarded, 3)

    async def test_reject_when_interval_includes_not_enqueued_id(self) -> None:
        self.queue.enqueue(build_packet(0), 0)  # only id 1 exists
        with self.assertRaises(RetractionError) as ctx:
            self.queue.retract_range(1, 3)
        self.assertEqual(ctx.exception.reason, NOT_ENQUEUED)
        self.assertEqual(ctx.exception.queue_id, 2)
        # Atomic: id 1 must remain pending and still be sent.
        await self.advance(0.2)
        self.assertEqual(self.rtp_seqs(), [0])

    async def test_reject_when_interval_includes_already_sent_id(self) -> None:
        for i in range(3):
            self.queue.enqueue(build_packet(i), i)
        await self.advance(0.2)  # all three sent
        self.queue.enqueue(build_packet(3), 3)  # id 4 pending
        with self.assertRaises(RetractionError) as ctx:
            self.queue.retract_range(2, 4)
        self.assertEqual(ctx.exception.reason, ALREADY_SENT)
        self.assertEqual(ctx.exception.queue_id, 2)
        # The pending id 4 must be untouched by the failed request.
        self.assertEqual([p["queue_id"] for p in self.queue.pending()], [4])

    async def test_reject_when_interval_includes_already_retracted_id(self) -> None:
        for i in range(4):
            self.queue.enqueue(build_packet(i), i)
        self.queue.retract_range(2, 2)
        with self.assertRaises(RetractionError) as ctx:
            self.queue.retract_range(1, 3)
        self.assertEqual(ctx.exception.reason, ALREADY_RETRACTED)
        self.assertEqual(ctx.exception.queue_id, 2)
        # ids 1 and 3 were part of the rejected batch and stay pending.
        self.assertEqual([p["queue_id"] for p in self.queue.pending()], [1, 3, 4])
        await self.advance(0.2)
        self.assertEqual(self.rtp_seqs(), [0, 2, 3])

    async def test_malformed_intervals_raise_value_error(self) -> None:
        with self.assertRaises(ValueError):
            self.queue.retract_range(0, 2)
        with self.assertRaises(ValueError):
            self.queue.retract_range(3, 2)

    async def test_retraction_history_keeps_precise_reasons_after_prune(self) -> None:
        # Fill and flush enough entries to force head pruning, then confirm a
        # retraction targeting an old id still reports the exact reason.
        for i in range(10):
            self.queue.enqueue(build_packet(i), i)
        await self.advance(0.25)
        self.assertEqual(self.rtp_seqs(), list(range(10)))
        with self.assertRaises(RetractionError) as ctx:
            self.queue.retract_range(1, 1)
        self.assertEqual(ctx.exception.reason, ALREADY_SENT)

    async def test_single_id_retraction_at_head_keeps_remaining_schedule(self) -> None:
        self.queue.enqueue(build_packet(0), 0)
        self.queue.enqueue(build_packet(1), 1)
        self.queue.retract_range(1, 1)  # pull the head before it is due
        snapshot = self.queue.snapshot()
        self.assertEqual(snapshot["oldest_pending_queue_id"], 2)
        await self.advance(0.25)
        self.assertEqual(self.rtp_seqs(), [1])


class TestEngineDelayMode(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = make_settings()
        self.transport = RecordingTransport()

    def make_engine(self) -> RelayEngine:
        return RelayEngine(self.settings, self.transport)

    def feed(self, engine: RelayEngine, seq: int, path: str = "A") -> None:
        engine.handle_packet(build_packet(seq), path)

    def forwarded_seqs(self) -> list[int]:
        return [sequence_number(data) for data, _ in self.transport.sent]

    async def test_first_packet_is_held_not_forwarded_immediately(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 100)
        self.assertEqual(self.forwarded_seqs(), [])
        pending = engine.pending_packets()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["queue_id"], 1)
        self.assertEqual(pending[0]["rtp_seq"], 100)
        self.assertEqual(engine.status()["next_seq"], 101)
        await asyncio.sleep(0.25)
        self.assertEqual(self.forwarded_seqs(), [100])
        await engine.aclose()

    async def test_status_forwarded_counts_only_udp_handoffs(self) -> None:
        engine = self.make_engine()
        for seq in range(5):
            self.feed(engine, seq)
        await asyncio.sleep(0.05)
        status = engine.status()
        self.assertEqual(status["stats"]["forwarded"], 0)
        self.assertEqual(status["delay"]["pending_count"], 5)
        # Retract two packets: they must not appear in skipped, only in
        # retracted, and forwarded must still count real sends later.
        engine.retract_range(2, 3)
        self.assertEqual(engine.status()["stats"]["skipped"], 0)
        self.assertEqual(engine.status()["stats"]["retracted"], 2)
        await asyncio.sleep(0.25)
        status = engine.status()
        self.assertEqual(status["stats"]["forwarded"], 3)
        # Queue ids 2,3 carry RTP seqs 1,2; the original packets keep RTP
        # order on the wire, so the sink simply never sees RTP seq 1 and 2.
        self.assertEqual(self.forwarded_seqs(), [0, 3, 4])
        await engine.aclose()

    async def test_gap_timeout_skip_still_works_under_delay(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        self.feed(engine, 2)  # gap at 1
        # The 40 ms gap wait fires well inside the 100 ms hold, so seq 1 is
        # skipped while both received packets are still queued, not sent.
        await asyncio.sleep(0.07)
        self.assertEqual(engine.recent_skipped(), [1])
        self.assertEqual(self.forwarded_seqs(), [])
        pending = engine.pending_packets()
        self.assertEqual([p["rtp_seq"] for p in pending], [0, 2])
        await asyncio.sleep(0.2)
        self.assertEqual(self.forwarded_seqs(), [0, 2])
        # Gap skips are not operator retractions.
        self.assertEqual(engine.status()["stats"]["retracted"], 0)
        await engine.aclose()

    async def test_silence_then_full_drain_with_timer_only(self) -> None:
        engine = self.make_engine()
        for seq in range(6):
            self.feed(engine, seq)
        # Upstream stops here. No more input; the delay queue must drain all.
        deadline = asyncio.get_running_loop().time() + 1
        while engine.pending_packets() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        self.assertEqual(self.forwarded_seqs(), list(range(6)))
        status = engine.status()
        self.assertEqual(status["stats"]["forwarded"], 6)
        self.assertEqual(status["delay"]["pending_count"], 0)
        await engine.aclose()

    async def test_retract_across_rtp_wrap_keeps_distinct_queue_ids(self) -> None:
        engine = self.make_engine()
        start = SEQ_MODULUS - 2
        # Feed five packets crossing the 16-bit wrap.
        for offset in range(5):
            self.feed(engine, (start + offset) % SEQ_MODULUS)
        pending = engine.pending_packets()
        self.assertEqual([p["queue_id"] for p in pending], [1, 2, 3, 4, 5])
        self.assertEqual([p["rtp_seq"] for p in pending],
                         [(start + offset) % SEQ_MODULUS for offset in range(5)])
        # Retract the wrap point itself (rtp seq 0) plus its neighbour.
        result = engine.retract_range(3, 4)
        self.assertEqual(result["rtp_seqs"], [0, 1])
        await asyncio.sleep(0.25)
        self.assertEqual(self.forwarded_seqs(),
                         [start, (start + 1) % SEQ_MODULUS,
                          (start + 4) % SEQ_MODULUS])
        await engine.aclose()

    async def test_retraction_rejection_is_counted(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        with self.assertRaises(RetractionError):
            engine.retract_range(1, 5)  # ids 2..5 not enqueued
        self.assertEqual(engine.stats.retraction_rejections, 1)
        # Rejected request changed nothing.
        self.assertEqual(len(engine.pending_packets()), 1)
        await engine.aclose()


class TestEngineDelayDisabled(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = make_settings(delay_enabled=False)
        self.transport = RecordingTransport()

    async def test_immediate_forward_path_unchanged(self) -> None:
        engine = RelayEngine(self.settings, self.transport)
        self.assertFalse(engine.delay_enabled)
        engine.handle_packet(build_packet(7), "A")
        self.assertEqual([sequence_number(d) for d, _ in self.transport.sent], [7])
        self.assertEqual(engine.pending_packets(), [])
        self.assertEqual(engine.status()["delay"], {"enabled": False})
        with self.assertRaises(RuntimeError):
            engine.retract_range(1, 1)
        await engine.aclose()


if __name__ == "__main__":
    unittest.main()
