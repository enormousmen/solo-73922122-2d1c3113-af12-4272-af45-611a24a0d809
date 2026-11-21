"""FastAPI query and control surface."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .delay_queue import ALREADY_RETRACTED, ALREADY_SENT, NOT_ENQUEUED, RetractionError
from .relay import RelayEngine


class RetractRequest(BaseModel):
    # Closed, inclusive interval of monotonic queue ids (待播编号).
    start: int = Field(ge=1, description="first queue id to retract (inclusive)")
    end: int = Field(ge=1, description="last queue id to retract (inclusive)")


_REJECTION_STATUS = {
    ALREADY_SENT: ("already_sent", 409),
    ALREADY_RETRACTED: ("already_retracted", 409),
    NOT_ENQUEUED: ("not_enqueued", 409),
}


def create_app(engine: RelayEngine) -> FastAPI:
    app = FastAPI(
        title="Dual-path RTP relay",
        description="双路 RTP 丢包补位中继：查询跳过序号、运行状态与延播撤播",
        version="1.1.0",
    )

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/skipped")
    async def skipped() -> dict:
        """The most recent 64 skipped RTP sequence numbers, newest first.

        Operator retractions are intentionally never listed here: ``/skipped``
        only tracks upstream packets the relay had to give up on after the
        gap wait, not packets deliberately pulled by the operator.
        """
        return {"skipped": engine.recent_skipped()}

    @app.get("/status")
    async def status() -> dict:
        """Full runtime snapshot (counters, pending sequence, buffer size)."""
        return engine.status()

    @app.get("/pending")
    async def pending() -> dict:
        """Packets still held in the delay queue, queue id ascending.

        Each item pairs the monotonic, never-reused queue id with the
        wrapping RTP sequence number of the original datagram. Empty when
        delay-broadcast mode is disabled.
        """
        return {
            "delay_enabled": engine.delay_enabled,
            "pending": engine.pending_packets(),
        }

    @app.post("/retract")
    async def retract(req: RetractRequest) -> dict:
        """Retract a closed queue-id interval, all-or-nothing.

        Succeeds only when *every* id in [start, end] is currently pending.
        If any id was already sent, already retracted, or has not been
        enqueued yet, the whole interval is rejected and nothing changes.
        """
        if not engine.delay_enabled:
            raise HTTPException(
                status_code=400,
                detail={"error": "delay_disabled",
                        "message": "relay is not running in delay-broadcast mode"},
            )
        try:
            return engine.retract_range(req.start, req.end)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail={"error": "invalid_range", "message": str(exc)})
        except RetractionError as exc:
            code, http_status = _REJECTION_STATUS[exc.reason]
            raise HTTPException(
                status_code=http_status,
                detail={
                    "error": code,
                    "queue_id": exc.queue_id,
                    "message": str(exc),
                    "range": {"start": req.start, "end": req.end},
                },
            )

    return app
