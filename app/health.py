"""Readiness, liveness and the counters behind ``/status``.

"The container is up" was the only signal this bridge produced. It says nothing
about whether MAX authorised us, whether a chat worker is still alive, or
whether anything has been delivered in the last hour (audit O03). The registry
here is what ``/status`` reads and what an external check can be pointed at.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class LinkState(str, Enum):
    """Explicit connection lifecycle — an open socket is not readiness.

    ``AUTH_REQUIRED`` is only entered on a *confirmed* rejection from MAX. A
    timeout means "authorisation did not complete", which is a different claim
    and gets a different message (audit 14.2/14.5).
    """

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    HANDSHAKING = "handshaking"
    AUTHENTICATING = "authenticating"
    READY = "ready"
    RECONNECT_BACKOFF = "reconnect_backoff"
    AUTH_REQUIRED = "auth_required"
    STOPPING = "stopping"
    STOPPED = "stopped"

    @property
    def is_ready(self) -> bool:
        return self is LinkState.READY


@dataclass
class Counters:
    dispatches: int = 0
    accepted: int = 0
    replays: int = 0
    edits: int = 0
    quarantined: int = 0
    delivered: int = 0
    failed: int = 0
    uncertain: int = 0
    reconnects: int = 0
    rpc_timeouts: int = 0
    downloads: int = 0
    download_bytes: int = 0
    blocked_urls: int = 0

    def bump(self, name: str, amount: int = 1) -> None:
        setattr(self, name, getattr(self, name, 0) + amount)

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass
class Health:
    """Single mutable snapshot of "is this bridge actually working"."""

    state: LinkState = LinkState.DISCONNECTED
    state_since: float = field(default_factory=time.monotonic)
    last_frame_at: float | None = None
    last_delivery_at: float | None = None
    last_auth_at: float | None = None
    last_error: str | None = None
    incident_started_at: float | None = None
    # How long the last outage lasted, measured when it ends. Needed because
    # `incident_started_at` is cleared the moment the link goes ready, and the
    # thing that has to report the gap runs after that (audit R07).
    last_outage_sec: float | None = None
    counters: Counters = field(default_factory=Counters)
    worker_count: int = 0
    queue_probe: Callable[[], dict[str, Any]] | None = None
    # Set on every transition so the health publisher can react to a state
    # change immediately rather than on its next tick.
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    # Level-triggered twin of ``state.is_ready`` for code that has to wait for
    # the link rather than poll it.
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)

    def set_state(self, state: LinkState, error: str | None = None) -> None:
        if state is not self.state:
            self.state = state
            self.state_since = time.monotonic()
            self.changed.set()
            if state.is_ready:
                self.ready_event.set()
            else:
                self.ready_event.clear()
        if error:
            self.last_error = error
        if state.is_ready:
            self.last_auth_at = time.time()
            if self.incident_started_at is not None:
                self.last_outage_sec = time.time() - self.incident_started_at
            self.incident_started_at = None
        elif state in (LinkState.DISCONNECTED, LinkState.RECONNECT_BACKOFF,
                       LinkState.AUTH_REQUIRED) and self.incident_started_at is None:
            self.incident_started_at = time.time()

    def note_frame(self) -> None:
        self.last_frame_at = time.monotonic()

    def note_delivery(self) -> None:
        self.last_delivery_at = time.time()
        self.counters.bump("delivered")

    @property
    def ready(self) -> bool:
        return self.state.is_ready

    @property
    def state_age(self) -> float:
        return time.monotonic() - self.state_since

    @property
    def incident_age(self) -> float | None:
        if self.incident_started_at is None:
            return None
        return time.time() - self.incident_started_at

    def snapshot(self) -> dict[str, Any]:
        last_frame_age = (
            time.monotonic() - self.last_frame_at
            if self.last_frame_at is not None else None
        )
        data: dict[str, Any] = {
            "state": self.state.value,
            "state_age": round(self.state_age, 1),
            "ready": self.ready,
            "last_frame_age": round(last_frame_age, 1) if last_frame_age is not None else None,
            "last_delivery_at": self.last_delivery_at,
            "incident_age": self.incident_age,
            "last_outage_sec": self.last_outage_sec,
            "last_error": self.last_error,
            "workers": self.worker_count,
            "counters": self.counters.as_dict(),
        }
        if self.queue_probe is not None:
            try:
                data["queues"] = self.queue_probe()
            except Exception:
                data["queues"] = {"error": "probe failed"}
        return data


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    seconds = max(0.0, float(seconds))
    if seconds < 60:
        return f"{seconds:.0f}с"
    if seconds < 3600:
        return f"{seconds / 60:.0f}м"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}ч"
    return f"{seconds / 86400:.1f}д"
