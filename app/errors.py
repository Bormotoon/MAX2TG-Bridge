"""Typed failures shared by the transport, the delivery pipeline and the senders.

The audit's central complaint was that "it returned" and "it was delivered" had
become the same thing: ``_retry()`` swallowed the final exception and returned
``None``, callers ignored that, and the log claimed success. Everything that can
fail now says *how* it failed, and the three answers that matter are kept apart:

* it definitely did not happen        → :data:`Outcome.TRANSIENT` / :data:`Outcome.PERMANENT`
* it definitely happened              → :data:`Outcome.DELIVERED`
* nobody knows whether it happened    → :data:`Outcome.UNCERTAIN`

The last one is the honest answer after a timeout on a request Telegram may well
have accepted, and it is the reason the pipeline never blind-retries a whole
message: parts that are already confirmed stay confirmed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class BridgeError(Exception):
    """Base class for every error this project raises on purpose."""


class ConnectionLost(BridgeError):
    """The WebSocket died while an RPC was in flight.

    Raised *instead of* cancelling the caller. Cancelling pending futures used
    to propagate ``CancelledError`` into whichever task was awaiting them, and
    a per-chat worker that caught only ``Exception`` died silently — its queue
    then stayed in the registry, so the chat stopped forwarding until restart
    (audit R01).
    """


class NotReady(BridgeError):
    """A command was issued before the connection finished authenticating."""


class AuthFailed(BridgeError):
    """MAX rejected our credentials — retrying with the same token is pointless."""


class AuthTimeout(BridgeError):
    """Handshake or auth did not complete within its deadline.

    Deliberately distinct from :class:`AuthFailed`: silence proves nothing about
    the token's validity, and telling the owner "your token is invalid" on a
    network stall is how you get a token rotated for no reason.
    """


class MaxRpcError(BridgeError):
    """MAX answered a request with ``cmd=3``."""

    def __init__(self, opcode: int, payload: dict):
        self.opcode = opcode
        self.payload = payload or {}
        super().__init__(f"MAX error on opcode {opcode}: {self.describe()}")

    def describe(self) -> str:
        p = self.payload
        return str(
            p.get("localizedMessage")
            or p.get("message")
            or p.get("error")
            or p
        )


class MediaTooLarge(BridgeError):
    """A download exceeded the configured per-file or per-process byte budget."""


class FileTooLarge(MediaTooLarge):
    """This one file is over the per-file limit and always will be.

    Kept apart from an exhausted spool budget, which frees up as other
    downloads finish: retrying that is right, retrying this never helps.
    """

    def __init__(self, message: str, size: int | None = None):
        super().__init__(message)
        self.size = size


class UnsafeUrl(BridgeError):
    """A URL was refused by the fetch policy (scheme, host or resolved IP)."""


class StoreCorrupted(BridgeError):
    """Persistent state failed validation and must not be silently replaced."""


class Outcome(str, Enum):
    """What actually became of one send attempt."""

    DELIVERED = "delivered"
    TRANSIENT = "transient_failed"
    PERMANENT = "permanent_failed"
    UNCERTAIN = "uncertain"

    @property
    def ok(self) -> bool:
        return self is Outcome.DELIVERED

    @property
    def retryable(self) -> bool:
        return self is Outcome.TRANSIENT


@dataclass
class SendResult:
    """The result of one logical send to a destination.

    ``message_ids`` are confirmed destination IDs (Telegram message IDs, or the
    MAX message ID for the other direction). They are what lets a partially
    delivered batch be resumed instead of re-sent whole.
    """

    outcome: Outcome
    message_ids: list[int] = field(default_factory=list)
    error: str | None = None
    retry_after: float | None = None

    @property
    def ok(self) -> bool:
        return self.outcome.ok

    def __bool__(self) -> bool:  # keeps `if result:` reading naturally
        return self.ok

    @classmethod
    def delivered(cls, message_ids: list[int] | None = None) -> SendResult:
        return cls(Outcome.DELIVERED, list(message_ids or []))

    @classmethod
    def transient(cls, error: str, retry_after: float | None = None) -> SendResult:
        return cls(Outcome.TRANSIENT, error=error, retry_after=retry_after)

    @classmethod
    def permanent(cls, error: str) -> SendResult:
        return cls(Outcome.PERMANENT, error=error)

    @classmethod
    def uncertain(cls, error: str) -> SendResult:
        return cls(Outcome.UNCERTAIN, error=error)


@dataclass
class AttachResult:
    """Outcome of forwarding one attachment.

    ``caption_delivered`` is tracked separately from ``attachment_delivered``
    because several destinations (stickers, plain-text placeholders) carry no
    caption at all. Treating "an attachment existed" as "its text went out" is
    what silently dropped message bodies before (audit M06).
    """

    attachment_delivered: bool = False
    caption_delivered: bool = False
    message_ids: list[int] = field(default_factory=list)
    outcome: Outcome = Outcome.PERMANENT
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.attachment_delivered

    @property
    def should_retry(self) -> bool:
        return self.outcome in (Outcome.TRANSIENT, Outcome.UNCERTAIN)


def describe_exception(exc: BaseException) -> str:
    """One-line, log-safe rendering of an exception."""
    text = str(exc).strip()
    name = type(exc).__name__
    return f"{name}: {text}" if text else name


def merge_outcomes(outcomes: list[Outcome]) -> Outcome:
    """Collapse the outcomes of several parts into one job-level outcome.

    Severity order matters: a single uncertain part makes the whole job
    uncertain (we must not blind-retry it), while a transient failure keeps the
    job retryable and a permanent one only wins if nothing worse happened.
    """
    if not outcomes:
        return Outcome.DELIVERED
    if Outcome.UNCERTAIN in outcomes:
        return Outcome.UNCERTAIN
    if Outcome.TRANSIENT in outcomes:
        return Outcome.TRANSIENT
    if Outcome.PERMANENT in outcomes:
        return Outcome.PERMANENT
    return Outcome.DELIVERED


def coerce_retry_after(value: Any) -> float | None:
    """Normalise ``RetryAfter.retry_after``, which may be int or ``timedelta``.

    python-telegram-bot changed the type across the versions this project claims
    to support; passing a ``timedelta`` straight to ``asyncio.sleep`` raises
    (audit O02).
    """
    if value is None:
        return None
    seconds = getattr(value, "total_seconds", None)
    if callable(seconds):
        try:
            return float(seconds())
        except Exception:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
