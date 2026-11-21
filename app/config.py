"""Runtime configuration loaded from environment variables.

All knobs have sensible defaults so ``python -m app.main`` works out of the
box for the local demo.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name} must be a number, got {raw!r}") from exc


_TRUE_TOKENS = {"1", "true", "yes", "on", "enabled"}
_FALSE_TOKENS = {"0", "false", "no", "off", "disabled", ""}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ValueError(f"environment variable {name} must be a boolean, got {raw!r}")


@dataclass(frozen=True)
class Settings:
    # Two ingress UDP ports carrying the same programme.
    port_a: int
    port_b: int
    # Where the recovered, in-order stream is sent (the playout machine).
    forward_host: str
    forward_port: int
    # FastAPI control/query endpoint.
    api_host: str
    api_port: int
    # Only packets within this many positions of the pending sequence are kept.
    window: int
    # Gap wait budget, milliseconds, starting at the first later packet.
    gap_timeout_ms: int
    # Delay-broadcast mode, opt-in at startup: every merged packet is held in
    # a delay queue for delay_seconds before it may reach the playout machine,
    # giving the operator a window to retract not-yet-sent packets.
    delay_enabled: bool = False
    delay_seconds: float = 8.0
    # RTP sequence numbers are 16-bit.
    seq_modulus: int = 1 << 16

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            port_a=_env_int("UDP_PORT_A", 9100),
            port_b=_env_int("UDP_PORT_B", 9101),
            forward_host=os.environ.get("FORWARD_HOST", "127.0.0.1"),
            forward_port=_env_int("FORWARD_PORT", 9200),
            api_host=os.environ.get("API_HOST", "0.0.0.0"),
            api_port=_env_int("API_PORT", 8000),
            window=_env_int("RELAY_WINDOW", 64),
            gap_timeout_ms=_env_int("GAP_TIMEOUT_MS", 80),
            delay_enabled=_env_bool("DELAY_ENABLED", False),
            delay_seconds=_env_float("DELAY_SECONDS", 8.0),
        )

    def validate(self) -> None:
        for name, value in {
            "UDP_PORT_A": self.port_a,
            "UDP_PORT_B": self.port_b,
            "FORWARD_PORT": self.forward_port,
            "API_PORT": self.api_port,
        }.items():
            if not 1 <= value <= 65535:
                raise ValueError(f"{name} must be in 1..65535, got {value}")
        if self.port_a == self.port_b:
            raise ValueError("UDP_PORT_A and UDP_PORT_B must be different")
        if self.window < 2 or self.window >= self.seq_modulus:
            raise ValueError("RELAY_WINDOW must be in 2..65535")
        if self.gap_timeout_ms <= 0:
            raise ValueError("GAP_TIMEOUT_MS must be positive")
        if self.delay_seconds <= 0 or self.delay_seconds > 3600:
            raise ValueError("DELAY_SECONDS must be in (0, 3600]")
