"""End-to-end tests for delay-broadcast mode over real UDP and HTTP."""
from __future__ import annotations

import asyncio
import json
import socket
import unittest

import uvicorn

from app.api import create_app
from app.config import Settings
from app.relay import RelayEngine
from app.rtp import build_packet, is_rtp_packet, sequence_number
from app.udp_transport import open_forwarder, open_ingress


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class SinkProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.seqs: list[int] = []

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        if is_rtp_packet(data):
            self.seqs.append(sequence_number(data))


class DelayEndToEndTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        port_a, port_b = _free_udp_port(), _free_udp_port()
        fwd_port = _free_udp_port()
        api_port = _free_tcp_port()
        # 200 ms hold: long enough to run retractions deterministically,
        # short enough to keep the suite fast.
        self.settings = Settings(
            port_a=port_a, port_b=port_b,
            forward_host="127.0.0.1", forward_port=fwd_port,
            api_host="127.0.0.1", api_port=api_port,
            window=64, gap_timeout_ms=40,
            delay_enabled=True, delay_seconds=0.2,
        )
        self.settings.validate()

        self.forward_transport = await open_forwarder()
        self.engine = RelayEngine(self.settings, self.forward_transport)
        self.ingress_a = await open_ingress(self.engine, port_a, "A")
        self.ingress_b = await open_ingress(self.engine, port_b, "B")

        loop = asyncio.get_running_loop()
        self.sink = SinkProtocol()
        # Pre-build the socket with SO_REUSEADDR so rapid teardown/rebind of
        # ephemeral loopback ports between test cases cannot race.
        sink_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sink_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sink_sock.bind(("127.0.0.1", fwd_port))
        sink_sock.setblocking(False)
        self.sink_transport, _ = await loop.create_datagram_endpoint(
            lambda: self.sink, sock=sink_sock)

        config = uvicorn.Config(create_app(self.engine),
                                host="127.0.0.1", port=api_port,
                                log_level="warning", lifespan="off")
        self.server = uvicorn.Server(config)
        self.server_task = asyncio.create_task(self.server.serve())
        self.api_base = ("127.0.0.1", api_port)
        for _ in range(50):
            try:
                if (await self._get("/health"))["status"] == "ok":
                    break
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.02)
        else:
            raise RuntimeError("test API server did not become ready")

    async def asyncTearDown(self) -> None:
        self.server_task.cancel()
        try:
            await self.server_task
        except asyncio.CancelledError:
            pass
        await self.engine.aclose()
        self.ingress_a.close()
        self.ingress_b.close()
        self.forward_transport.close()
        self.sink_transport.close()
        # Let the loop finish socket teardown so an ephemeral port reused by
        # the next test's sink cannot swallow this test's lingering datagrams.
        await asyncio.sleep(0.05)

    async def _get(self, path: str) -> dict:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(*self.api_base), timeout=2)
        try:
            writer.write(f"GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n".encode())
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(), timeout=2)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
        body = raw.split(b"\r\n\r\n", 1)[1]
        return json.loads(body)

    async def _post(self, path: str, payload: dict) -> tuple[int, dict]:
        """Minimal async HTTP/1.1 POST returning (status, parsed JSON)."""
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(*self.api_base), timeout=2)
        body = json.dumps(payload).encode()
        try:
            writer.write(
                f"POST {path} HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(), timeout=2)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
        head, _, resp_body = raw.partition(b"\r\n\r\n")
        status_line = head.split(b"\r\n", 1)[0].decode()
        status_code = int(status_line.split()[1])
        return status_code, json.loads(resp_body)

    async def _send_path(self, port: int, packets: list[bytes]) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=("127.0.0.1", port))
        for data in packets:
            transport.sendto(data)
        await asyncio.sleep(0.05)
        transport.close()

    async def _wait_drained(self, timeout: float = 2.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while self.engine.pending_packets() and loop.time() < deadline:
            await asyncio.sleep(0.01)
        self.assertEqual(self.engine.pending_packets(), [])
        # All hand-offs happened; give the loopback stack one last beat to
        # deliver datagrams to the sink socket before assertions/teardown.
        await asyncio.sleep(0.1)

    async def test_pending_lists_queue_ids_and_rtp_seqs(self) -> None:
        await self._send_path(self.settings.port_a,
                              [build_packet(s) for s in range(0, 6)])
        pending = (await self._get("/pending"))["pending"]
        self.assertEqual([p["queue_id"] for p in pending], [1, 2, 3, 4, 5, 6])
        self.assertEqual([p["rtp_seq"] for p in pending], list(range(0, 6)))
        status = await self._get("/status")
        self.assertTrue(status["delay"]["enabled"])
        # Nothing handed to UDP before the hold elapses.
        self.assertEqual(status["stats"]["forwarded"], 0)
        self.assertEqual(self.sink.seqs, [])

    async def test_retract_interval_removes_packets_at_sink(self) -> None:
        await self._send_path(self.settings.port_a,
                              [build_packet(s) for s in range(0, 10)])
        # Retract queue ids 4..6 (RTP seqs 3,4,5) while everything is pending.
        code, resp = await self._post("/retract", {"start": 4, "end": 6})
        self.assertEqual(code, 200)
        self.assertTrue(resp["retracted"])
        self.assertEqual(resp["rtp_seqs"], [3, 4, 5])

        await self._wait_drained()
        self.assertEqual(self.sink.seqs,
                         [0, 1, 2, 6, 7, 8, 9])
        status = await self._get("/status")
        self.assertEqual(status["stats"]["forwarded"], 7)
        self.assertEqual(status["stats"]["retracted"], 3)
        # Operator retractions must never be counted as skips.
        self.assertEqual(status["stats"]["skipped"], 0)
        self.assertEqual((await self._get("/skipped"))["skipped"], [])

    async def test_rejections_are_atomic_and_reported_with_reason(self) -> None:
        await self._send_path(self.settings.port_a,
                              [build_packet(s) for s in range(0, 4)])
        # Not-enqueued id: only 1..4 exist, request reaches id 9.
        code, resp = await self._post("/retract", {"start": 1, "end": 9})
        self.assertEqual(code, 409)
        self.assertEqual(resp["detail"]["error"], "not_enqueued")
        self.assertEqual(resp["detail"]["queue_id"], 5)

        # Let the hold elapse: ids 1..4 are sent, in order, unchanged.
        await self._wait_drained()
        code, resp = await self._post("/retract", {"start": 1, "end": 2})
        self.assertEqual(code, 409)
        self.assertEqual(resp["detail"]["error"], "already_sent")
        self.assertEqual(resp["detail"]["queue_id"], 1)
        status = await self._get("/status")
        self.assertEqual(status["stats"]["retraction_rejections"], 2)
        self.assertEqual(status["stats"]["retracted"], 0)
        self.assertEqual(self.sink.seqs, [0, 1, 2, 3])

    async def test_already_retracted_id_rejects_whole_interval(self) -> None:
        await self._send_path(self.settings.port_a,
                              [build_packet(s) for s in range(0, 5)])
        code, _ = await self._post("/retract", {"start": 2, "end": 2})
        self.assertEqual(code, 200)
        # A wider interval overlapping the already-retracted id must fail,
        # leaving ids 1,3,4,5 pending and still schedulable.
        code, resp = await self._post("/retract", {"start": 1, "end": 4})
        self.assertEqual(code, 409)
        self.assertEqual(resp["detail"]["error"], "already_retracted")
        self.assertEqual(resp["detail"]["queue_id"], 2)
        await self._wait_drained()
        self.assertEqual(self.sink.seqs, [0, 2, 3, 4])

    async def test_silence_after_enqueue_still_drains_via_timers(self) -> None:
        await self._send_path(self.settings.port_a,
                              [build_packet(s) for s in range(0, 8)])
        # No more upstream traffic at all; the hold timers must drain all
        # pending packets without any new input.
        await self._wait_drained()
        self.assertEqual(self.sink.seqs, list(range(0, 8)))
        status = await self._get("/status")
        self.assertEqual(status["stats"]["forwarded"], 8)
        self.assertEqual(status["delay"]["pending_count"], 0)

    async def test_validation_errors_return_400(self) -> None:
        await self._send_path(self.settings.port_a, [build_packet(0)])
        code, resp = await self._post("/retract", {"start": 3, "end": 2})
        self.assertEqual(code, 400)
        self.assertEqual(resp["detail"]["error"], "invalid_range")
        # Pydantic rejects ge=1 violations with its own 422.
        code, _ = await self._post("/retract", {"start": 0, "end": 2})
        self.assertEqual(code, 422)


if __name__ == "__main__":
    unittest.main()
