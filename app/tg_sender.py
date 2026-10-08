"""Telegram delivery: every send says what happened to it.

The old sender returned ``None`` on every kind of failure and the public methods
returned nothing at all, so the pipeline logged "Forwarded" whether or not
anything reached Telegram (audit D01). Each method here returns a
:class:`~app.errors.SendResult` carrying the confirmed message IDs, and failures
are classified into the three answers that lead to different behaviour:
retry it, give up on it, or admit we do not know.

Two other fixes live here:

* **Long text is split, not truncated.** Cutting an HTML string at a byte offset
  lands inside ``&amp;`` or a tag, Telegram rejects the whole message, and the
  retry re-sends the same broken payload (audit M07). The splitter tracks open
  tags and re-opens them in the next part.
* **Albums are chunked so no chunk has one item.** 11 photos used to become
  ``[10, 1]`` and the trailing one-item ``sendMediaGroup`` is rejected outright,
  losing the last photo (audit M03).
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import html
import io
import logging
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from telegram import (
    Bot,
    InputFile,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)
from telegram.constants import ParseMode
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TimedOut,
)
from telegram.request import HTTPXRequest

from app.errors import Outcome, SendResult, coerce_retry_after, describe_exception
from app.net import SpoolFile
from app.topics import is_topic_gone_error

log = logging.getLogger(__name__)

TG_MAX_LENGTH = 4096
TG_CAPTION_MAX = 1024
TG_MEDIA_GROUP_MAX = 10
MAX_RETRIES = 3

_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9]*)([^>]*)>")
_ENTITY_RE = re.compile(r"&(?:#\d+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]*);")


# ── HTML-aware splitting ───────────────────────────────────────────

@dataclass
class _Token:
    kind: str          # "text" | "open" | "close"
    raw: str
    name: str = ""
    visible: int = 0


# Catch-up after an outage posts days of history at once. Pinging every member
# of the group for each of those messages is noise, so a delivery can ask for
# silence; every Bot API call below reads it. A context variable rather than a
# parameter, because the flag has to reach every send path without threading it
# through each signature.
_silent: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "tg_silent", default=False)


def is_silent() -> bool:
    return _silent.get()


@contextlib.contextmanager
def silent(enabled: bool = True):
    """Send everything inside this block with ``disable_notification``."""
    token = _silent.set(enabled)
    try:
        yield
    finally:
        _silent.reset(token)


def _tokenize(text: str) -> list[_Token]:
    tokens: list[_Token] = []
    pos = 0
    for match in _TAG_RE.finditer(text):
        if match.start() > pos:
            chunk = text[pos:match.start()]
            tokens.append(_Token("text", chunk, visible=_visible_len(chunk)))
        closing, name, _attrs = match.groups()
        tokens.append(_Token("close" if closing else "open", match.group(0),
                             name=name.lower()))
        pos = match.end()
    if pos < len(text):
        chunk = text[pos:]
        tokens.append(_Token("text", chunk, visible=_visible_len(chunk)))
    return tokens


def _visible_len(chunk: str) -> int:
    """Length as Telegram counts it: entities are one character, not five."""
    return len(_ENTITY_RE.sub("￿", chunk))


def _split_text_chunk(chunk: str, budget: int) -> tuple[str, str]:
    """Cut a text run at ``budget`` visible characters without breaking an entity."""
    if budget <= 0:
        return "", chunk
    taken: list[str] = []
    visible = 0
    i = 0
    last_break = None
    while i < len(chunk) and visible < budget:
        entity = _ENTITY_RE.match(chunk, i)
        if entity:
            piece = entity.group(0)
            i = entity.end()
        else:
            piece = chunk[i]
            i += 1
        taken.append(piece)
        visible += 1
        if piece in ("\n", " "):
            last_break = (len("".join(taken)), i)
    head = "".join(taken)
    rest = chunk[i:]
    # Prefer a word boundary if one is reasonably close to the end.
    if last_break and visible >= budget and last_break[0] >= budget * 0.6:
        head = head[:last_break[0]]
        rest = chunk[last_break[1]:]
    return head, rest


def split_html(text: str, limit: int = TG_MAX_LENGTH) -> list[str]:
    """Split HTML into parts Telegram will accept, keeping markup valid.

    Tags open at a split point are closed at the end of the part and re-opened at
    the start of the next one, so no part is ever handed to Telegram with an
    unbalanced tag.
    """
    if not text:
        return []
    if _visible_len(text) <= limit:
        return [text]

    parts: list[str] = []
    open_stack: list[_Token] = []
    current: list[str] = []
    used = 0

    def flush() -> None:
        nonlocal current, used
        if not current and not open_stack:
            return
        body = "".join(current)
        closing = "".join(f"</{t.name}>" for t in reversed(open_stack))
        candidate = (body + closing).strip()
        if candidate:
            parts.append(body + closing)
        current = [t.raw for t in open_stack]
        used = 0

    for token in _tokenize(text):
        if token.kind == "open":
            current.append(token.raw)
            open_stack.append(token)
            continue
        if token.kind == "close":
            current.append(token.raw)
            for i in range(len(open_stack) - 1, -1, -1):
                if open_stack[i].name == token.name:
                    open_stack.pop(i)
                    break
            continue

        chunk = token.raw
        while chunk:
            budget = limit - used
            if budget <= 0:
                flush()
                continue
            if _visible_len(chunk) <= budget:
                current.append(chunk)
                used += _visible_len(chunk)
                break
            head, chunk = _split_text_chunk(chunk, budget)
            if not head:
                flush()
                continue
            current.append(head)
            used += _visible_len(head)
            flush()

    body = "".join(current)
    closing = "".join(f"</{t.name}>" for t in reversed(open_stack))
    if body.strip():
        parts.append(body + closing)
    return [p for p in parts if p.strip()]


def chunk_media(items: list, max_size: int = TG_MEDIA_GROUP_MAX) -> list[list]:
    """Split an album into balanced chunks, never leaving one item alone."""
    count = len(items)
    if count <= max_size:
        return [items] if items else []
    groups = -(-count // max_size)          # ceil
    base, remainder = divmod(count, groups)
    chunks: list[list] = []
    start = 0
    for i in range(groups):
        size = base + (1 if i < remainder else 0)
        chunks.append(items[start:start + size])
        start += size
    return chunks


# ── media items ────────────────────────────────────────────────────

@dataclass
class MediaItem:
    """One piece of media plus a way to re-read it on every retry."""

    kind: str                       # photo | video | animation | document | audio
    source: bytes | SpoolFile
    filename: str = "file"
    caption: str | None = None

    def input_file(self, *, attach: bool = False) -> InputFile:
        """A fresh handle per attempt: ``_retry`` re-invokes the factory and a
        consumed BytesIO/file object cannot be read twice.

        ``attach`` is required inside ``sendMediaGroup``: without it the file
        has no attach name, the request references ``attach://None`` and
        Telegram answers "media not found" — every album failed that way until
        October 2026.
        """
        if isinstance(self.source, SpoolFile):
            return InputFile(self.source.open(), filename=self.filename,
                             attach=attach)
        return InputFile(io.BytesIO(self.source), filename=self.filename,
                         attach=attach)

    @property
    def size(self) -> int:
        if isinstance(self.source, SpoolFile):
            return self.source.size
        return len(self.source)


_INPUT_MEDIA = {
    "photo": InputMediaPhoto,
    "video": InputMediaVideo,
    "audio": InputMediaAudio,
    "document": InputMediaDocument,
}


# ── error classification ───────────────────────────────────────────

def classify_error(exc: BaseException) -> SendResult:
    """Map a Telegram exception onto an outcome the pipeline can act on."""
    if isinstance(exc, RetryAfter):
        return SendResult.transient(
            describe_exception(exc),
            retry_after=coerce_retry_after(getattr(exc, "retry_after", None)))
    if isinstance(exc, TimedOut):
        # Telegram may well have accepted the request; re-sending risks a
        # duplicate and there is no idempotency key for sendMessage (audit D06).
        return SendResult.uncertain(describe_exception(exc))
    if isinstance(exc, Forbidden):
        return SendResult.permanent(describe_exception(exc))
    if isinstance(exc, BadRequest):
        if is_topic_gone_error(exc):
            return SendResult.permanent(describe_exception(exc))
        # A malformed request will fail identically on every attempt.
        return SendResult.permanent(describe_exception(exc))
    if isinstance(exc, NetworkError):
        return SendResult.transient(describe_exception(exc))
    return SendResult.transient(describe_exception(exc))


def _ids(result: Any) -> list[int]:
    if result is None:
        return []
    if isinstance(result, (list, tuple)):
        return [m.message_id for m in result if getattr(m, "message_id", None)]
    mid = getattr(result, "message_id", None)
    return [mid] if mid else []


def partial_failure(result: SendResult, confirmed: Iterable[int]) -> SendResult:
    """Fold already-confirmed message IDs into a failed multi-part send.

    Once Telegram has accepted *part* of a logical message — an earlier text
    chunk, the first album group, the media a caption belongs to — a plain
    retry re-posts that part, because ``sendMessage`` has no idempotency key.
    So a transient failure stops being retryable here and becomes
    :data:`~app.errors.Outcome.UNCERTAIN`: the pipeline parks the job, it shows
    up in ``/status``, and the owner decides via ``/retry``. That is the same
    policy the bridge already applies to a timeout, and it is what the audit
    means by "a partially delivered batch is not blindly re-sent whole"
    (audit D06/M03, §13).

    With nothing confirmed there is no duplicate to fear, so the original
    outcome — usually a retryable one — is left alone.
    """
    seen = list(confirmed)
    ids = seen + [i for i in result.message_ids if i not in seen]
    if not seen:
        result.message_ids = ids
        return result
    if result.outcome is Outcome.TRANSIENT:
        return SendResult(Outcome.UNCERTAIN, ids, result.error)
    return SendResult(result.outcome, ids, result.error)


# python-telegram-bot defaults to a 5 s read timeout. Under flood control, or
# through a proxy, Telegram routinely answers slower than that, and every such
# timeout became an UNCERTAIN job that needs a manual /retry — eleven of them in
# the first minutes of the October 2026 catch-up.
_REQUEST_TIMEOUTS = {
    "connect_timeout": 15.0,
    "read_timeout": 30.0,
    "write_timeout": 30.0,
    "media_write_timeout": 120.0,
    "pool_timeout": 10.0,
}


class _GroupPacer:
    """Token bucket for Telegram's per-group limit (about 20 messages/minute).

    Reacting to ``RetryAfter`` alone is not enough: by the time Telegram says
    "flood", requests are already being answered slowly, and slow answers time
    out into UNCERTAIN. A live burst still goes out at once from the bucket;
    a long backlog spreads itself out instead of hitting the wall.
    """

    def __init__(self, per_minute: float, burst: int):
        self.rate = per_minute / 60.0
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: int = 1) -> None:
        if self.rate <= 0:
            return
        cost = min(float(cost), self.capacity)
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity,
                                  self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= cost:
                    self.tokens -= cost
                    return
                await asyncio.sleep((cost - self.tokens) / self.rate)


class TelegramSender:
    """Thin, explicit wrapper over the Bot API for one supergroup."""

    # Seconds between in-call retries, multiplied by the attempt number. An
    # attribute rather than a literal so tests can exercise the retry path
    # without spending six real seconds on every failing send.
    RETRY_BACKOFF_SEC = 2.0
    # Messages per minute into the group, and how many may go out back to back.
    # Telegram documents ~20/min, but at a sustained 20/min (plus a full
    # 20-message bucket) the catch-up still hit RetryAfter every minute.
    GROUP_MSGS_PER_MIN = 15.0
    GROUP_BURST = 5

    def __init__(self, token: str, chat_id: str,
                 proxy_url: str | None = None,
                 status_chat_id: str | None = None,
                 bot: Bot | None = None):
        if bot is not None:
            self._bot = bot
        else:
            self._bot = Bot(token=token, request=HTTPXRequest(
                proxy=proxy_url or None, **_REQUEST_TIMEOUTS))
        self._chat_id = int(chat_id)
        self._status_chat_id = int(status_chat_id) if status_chat_id else None
        self._bot_id: int | None = None
        self._pacer = _GroupPacer(self.GROUP_MSGS_PER_MIN, self.GROUP_BURST)

    @property
    def bot(self) -> Bot:
        return self._bot

    @property
    def chat_id(self) -> int:
        return self._chat_id

    @property
    def bot_id(self) -> int | None:
        return self._bot_id

    async def start(self) -> None:
        await self._bot.initialize()
        me = await self._bot.get_me()
        self._bot_id = me.id
        log.info("Telegram bot ready: @%s", me.username)

    async def stop(self) -> None:
        await self._bot.shutdown()

    async def preflight(self) -> list[str]:
        """Check the supergroup is usable before we start forwarding into it.

        Finding out at the first incoming message that the group is not a forum,
        or that the bot cannot manage topics, means the message is already in
        flight when the discovery happens (audit O03).
        """
        problems: list[str] = []
        try:
            chat = await self._bot.get_chat(self._chat_id)
        except Exception as exc:
            return [f"не удалось прочитать супергруппу {self._chat_id}: "
                    f"{describe_exception(exc)}"]
        if not getattr(chat, "is_forum", False):
            problems.append("в супергруппе не включены темы (Topics/форум)")
        try:
            me = await self._bot.get_chat_member(self._chat_id, self._bot.id)
        except Exception as exc:
            problems.append(f"не удалось проверить права бота: {describe_exception(exc)}")
            return problems
        if getattr(me, "status", None) != "administrator":
            problems.append("бот не админ супергруппы")
        elif not getattr(me, "can_manage_topics", False):
            problems.append("у бота нет права «Управление темами»")
        return problems

    # ── retry core ─────────────────────────────────────────────────

    async def _retry(self, factory: Callable[[], Any], *,
                     what: str, cost: int = 1,
                     paced: bool = True) -> SendResult:
        last = SendResult.transient("not attempted")
        for attempt in range(1, MAX_RETRIES + 1):
            if paced:
                await self._pacer.acquire(cost)
            try:
                raw = await factory()
            except Exception as exc:
                last = classify_error(exc)
                if last.outcome is Outcome.UNCERTAIN:
                    log.warning("%s: no answer from Telegram — treating as "
                                "uncertain, not retrying blindly (%s)",
                                what, last.error)
                    return last
                if last.outcome is Outcome.PERMANENT:
                    log.error("%s failed permanently: %s", what, last.error)
                    return last
                delay = (last.retry_after if last.retry_after is not None
                         else self.RETRY_BACKOFF_SEC * attempt)
                if attempt == MAX_RETRIES:
                    log.error("%s failed after %d attempts: %s", what,
                              MAX_RETRIES, last.error)
                    return last
                log.warning("%s failed (attempt %d/%d): %s — retrying in %.0fs",
                            what, attempt, MAX_RETRIES, last.error, delay)
                await asyncio.sleep(delay)
                continue
            return SendResult.delivered(_ids(raw))
        return last

    # ── text ───────────────────────────────────────────────────────

    async def send_text(self, text: str, thread_id: int | None = None,
                        *, reply_to: int | None = None,
                        disable_preview: bool = False) -> SendResult:
        """Send text, splitting it across messages when it is too long."""
        if not text:
            return SendResult.delivered([])
        parts = split_html(text, TG_MAX_LENGTH)
        ids: list[int] = []
        for index, part in enumerate(parts):
            result = await self._retry(
                lambda p=part, r=(reply_to if index == 0 else None):
                    self._bot.send_message(
                        chat_id=self._chat_id, text=p,
                        parse_mode=ParseMode.HTML,
                        message_thread_id=thread_id,
                        reply_to_message_id=r,
                        # The original may have been deleted in Telegram; the
                        # reply still has to arrive, just without the link.
                        allow_sending_without_reply=True,
                        disable_web_page_preview=disable_preview,
                        disable_notification=is_silent(),
                    ),
                what=f"send_message part {index + 1}/{len(parts)}",
            )
            if not result.ok:
                # Report what did land, and refuse to call a half-sent message
                # retryable — the parts already in the topic would be posted
                # again.
                return partial_failure(result, ids)
            ids.extend(result.message_ids)
        return SendResult.delivered(ids)

    # ── single media ───────────────────────────────────────────────

    async def send_media(self, item: MediaItem, thread_id: int | None = None,
                         *, caption: str | None = None,
                         reply_to: int | None = None) -> SendResult:
        """Send one attachment, with its caption split off when it is too long."""
        caption_text, overflow = self._split_caption(caption or item.caption or "")
        method = {
            "photo": self._bot.send_photo,
            "video": self._bot.send_video,
            "animation": self._bot.send_animation,
            "audio": self._bot.send_audio,
            "voice": self._bot.send_voice,
            "document": self._bot.send_document,
            "sticker": self._bot.send_sticker,
        }.get(item.kind, self._bot.send_document)
        arg = {
            "photo": "photo", "video": "video", "animation": "animation",
            "audio": "audio", "voice": "voice", "document": "document",
            "sticker": "sticker",
        }.get(item.kind, "document")

        def factory():
            kwargs: dict[str, Any] = {
                "chat_id": self._chat_id,
                arg: item.input_file(),
                "message_thread_id": thread_id,
                "disable_notification": is_silent(),
            }
            if reply_to is not None:
                kwargs["reply_to_message_id"] = reply_to
                kwargs["allow_sending_without_reply"] = True
            if item.kind != "sticker":
                kwargs["caption"] = caption_text or None
                kwargs["parse_mode"] = ParseMode.HTML
            return method(**kwargs)

        result = await self._retry(factory, what=f"send_{item.kind}")
        if not result.ok:
            return result
        ids = list(result.message_ids)
        if overflow:
            tail = await self.send_text(overflow, thread_id)
            # The media is already in the topic; retrying to recover the tail of
            # its caption would upload it a second time.
            if not tail.ok:
                return partial_failure(tail, ids)
            ids.extend(tail.message_ids)
        return SendResult.delivered(ids)

    async def send_album(self, items: list[MediaItem],
                         thread_id: int | None = None, *,
                         caption: str | None = None) -> SendResult:
        """Send an album, splitting it into chunks Telegram will accept."""
        if not items:
            return SendResult.delivered([])
        if len(items) == 1:
            return await self.send_media(items[0], thread_id, caption=caption)

        caption_text, overflow = self._split_caption(caption or "")
        ids: list[int] = []
        chunks = chunk_media(items, TG_MEDIA_GROUP_MAX)
        for chunk_index, chunk in enumerate(chunks):
            if len(chunk) == 1:
                # chunk_media avoids this, but a caller could hand us one item.
                result = await self.send_media(
                    chunk[0], thread_id,
                    caption=caption_text if chunk_index == 0 else None)
            else:
                def factory(chunk=chunk, first=(chunk_index == 0)):
                    media = []
                    for i, item in enumerate(chunk):
                        cls = _INPUT_MEDIA.get(item.kind, InputMediaDocument)
                        media.append(cls(
                            media=item.input_file(attach=True),
                            caption=(caption_text if (first and i == 0
                                                      and caption_text) else None),
                            parse_mode=ParseMode.HTML,
                        ))
                    return self._bot.send_media_group(
                        chat_id=self._chat_id, media=media,
                        message_thread_id=thread_id,
                        disable_notification=is_silent())

                # Telegram counts every item of an album against the limit.
                result = await self._retry(
                    factory, what=f"send_media_group {chunk_index + 1}/{len(chunks)}",
                    cost=len(chunk))
            if not result.ok:
                return partial_failure(result, ids)
            ids.extend(result.message_ids)

        if overflow:
            tail = await self.send_text(overflow, thread_id)
            if not tail.ok:
                return partial_failure(tail, ids)
            ids.extend(tail.message_ids)
        return SendResult.delivered(ids)

    async def send_location(self, latitude: float, longitude: float,
                            thread_id: int | None = None) -> SendResult:
        return await self._retry(
            lambda: self._bot.send_location(
                chat_id=self._chat_id, latitude=latitude, longitude=longitude,
                message_thread_id=thread_id, disable_notification=is_silent()),
            what="send_location",
        )

    async def send_contact(self, phone: str, first_name: str,
                           last_name: str | None = None,
                           thread_id: int | None = None) -> SendResult:
        return await self._retry(
            lambda: self._bot.send_contact(
                chat_id=self._chat_id, phone_number=phone,
                first_name=first_name or "—", last_name=last_name,
                message_thread_id=thread_id, disable_notification=is_silent()),
            what="send_contact",
        )

    @staticmethod
    def _split_caption(caption: str) -> tuple[str, str]:
        """Keep what fits in a caption; the rest becomes a follow-up message."""
        if not caption:
            return "", ""
        if _visible_len(caption) <= TG_CAPTION_MAX:
            return caption, ""
        parts = split_html(caption, TG_CAPTION_MAX)
        return parts[0], "".join(parts[1:])


class StatusNotifier:
    """Bridge status messages, delivered out of band.

    Reconnection used to ``await`` the status message, so a Telegram outage —
    including a ``RetryAfter`` sleep — held the MAX link down while we tried to
    announce that the MAX link was down (audit R10). Messages are queued here
    and a worker delivers them; nothing on the connection path waits.

    Throttling is per *incident*, on the monotonic clock. The old counter never
    reset, so after a few unrelated outages every future warning was suppressed
    for a day.
    """

    FIRST_REPEAT_SEC = 3600
    SECOND_REPEAT_SEC = 10800
    LATER_REPEAT_SEC = 86400

    def __init__(self, sender: TelegramSender, chat_id: int | None,
                 fallback_thread: int | None = None, queue_size: int = 64):
        self._sender = sender
        self._chat_id = chat_id
        self._fallback_thread = fallback_thread
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task | None = None
        self._incident_notices = 0
        self._last_notice_at: float | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._worker(), name="status-notifier")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None

    def notify(self, text: str) -> None:
        """Queue a status message. Never blocks, never raises."""
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            log.warning("Status queue is full — dropping: %s", text)

    def notify_incident(self, text: str) -> bool:
        """Queue a repeat warning for an ongoing incident, if throttling allows."""
        now = time.monotonic()
        if self._last_notice_at is not None:
            elapsed = now - self._last_notice_at
            if self._incident_notices == 1 and elapsed < self.FIRST_REPEAT_SEC:
                return False
            if self._incident_notices == 2 and elapsed < self.SECOND_REPEAT_SEC:
                return False
            if self._incident_notices >= 3 and elapsed < self.LATER_REPEAT_SEC:
                return False
        self._incident_notices += 1
        self._last_notice_at = now
        self.notify(text)
        return True

    def resolve_incident(self, text: str | None = None) -> None:
        """The incident is over: announce it once and reset the throttle."""
        had_incident = self._incident_notices > 0
        self._incident_notices = 0
        self._last_notice_at = None
        if had_incident and text:
            self.notify(text)

    async def _worker(self) -> None:
        while True:
            text = await self._queue.get()
            try:
                if self._chat_id is not None:
                    result = await self._sender._retry(
                        # Bound explicitly: the factory is re-invoked on retry
                        # and `text` is reassigned by the next loop iteration.
                        lambda body=text: self._sender.bot.send_message(
                            chat_id=self._chat_id, text=body,
                            parse_mode=ParseMode.HTML),
                        # A private chat: the group's flood budget is not
                        # this message's to spend.
                        what="status notification", paced=False)
                    if not result.ok:
                        # Deliberately no fallback to the shared supergroup:
                        # keeping these out of it is the point of the setting.
                        log.error("Could not deliver a status notification to "
                                  "%s (has the user pressed /start?): %s",
                                  self._chat_id, result.error)
                else:
                    await self._sender.send_text(
                        text, thread_id=self._fallback_thread)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Status notifier failed")
            finally:
                self._queue.task_done()


def escape(text: Any) -> str:
    """HTML-escape anything on its way into a Telegram message."""
    return html.escape(str(text if text is not None else ""), quote=False)


@dataclass
class OutboundBatch:
    """A prepared group of media plus the caption that belongs to it."""

    items: list[MediaItem] = field(default_factory=list)
    caption: str = ""

    def release(self) -> None:
        for item in self.items:
            if isinstance(item.source, SpoolFile):
                item.source.release()


def release_all(items: Iterable[MediaItem]) -> None:
    for item in items:
        if isinstance(item.source, SpoolFile):
            item.source.release()
