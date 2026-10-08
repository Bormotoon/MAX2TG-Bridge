"""MAX WebSocket transport: connection lifecycle, RPC, uploads, downloads.

What changed and why, in one place, because the rewrite is large:

* **A connection is an object, not a pile of attributes.** Every pending RPC,
  every upload future and every sequence number belongs to one
  :class:`_Connection`. A late ``finally`` from a dead connection can no longer
  delete a live connection's future that happens to share a sequence number
  (audit R03).
* **Futures are registered before the send, not after.** ``await send_str()``
  can yield, and the read loop could deliver a fast reply into a future that did
  not exist yet — the request then sat there until it timed out (audit R02).
* **Disconnect fails pending RPCs, it does not cancel them.** ``cancel()``
  raised ``CancelledError`` inside whatever task was awaiting, and a per-chat
  worker catching only ``Exception`` died for good (audit R01). They now get
  :class:`~app.errors.ConnectionLost`, which is an ordinary exception.
* **Readiness is a state, not a guess.** ``ready`` means a validated
  AUTH_SNAPSHOT arrived. Connect, handshake, auth and socket writes each have
  their own deadline, so none of them can hang forever (audit R04).
* **The watchdog runs on its own task.** A wedged write used to stop the
  heartbeat loop, which was also the thing supposed to notice the wedge
  (audit R05).
* **No dedup or queueing here.** Accepted events go to the durable journal in
  :mod:`app.pipeline`; this module's job ends at "validated and handed over".
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

import aiohttp

from app.errors import (
    AuthFailed,
    ConnectionLost,
    MaxRpcError,
    NotReady,
    describe_exception,
)
from app.health import Health, LinkState
from app.logging_utils import safe_url
from app.net import MediaSpool, SafeFetcher, SpoolFile
from app.store import chat_key, revision_hash

log = logging.getLogger(__name__)

DEBUG_DIR = "debug"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

_BROWSER_HEADERS = {
    "User-Agent": _USER_AGENT,
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131", "Not?A_Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

_WS_HEADERS = {
    **_BROWSER_HEADERS,
    "Origin": "https://web.max.ru",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_HTTP_HEADERS = {
    **_BROWSER_HEADERS,
    "Origin": "https://web.max.ru",
    "Referer": "https://web.max.ru/",
    "Accept": "*/*",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    # aiohttp ships without brotli/zstd by default and the MAX upload server
    # honours whatever we advertise, so asking for them yields a body we cannot
    # decode.
    "Accept-Encoding": "gzip, deflate",
}

# Errors MAX returns when the credential itself is no longer good. Anything
# outside this set is treated as "authorisation did not complete", which is a
# transport problem until proven otherwise.
_AUTH_REJECTED_MARKERS = (
    "login.token", "token.invalid", "not.authorized", "unauthorized",
    "auth.failed", "session.expired", "logout",
)


def _log_task_exception(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        log.error("Unhandled exception in background task",
                  exc_info=task.exception())


class OpCode(IntEnum):
    HEARTBEAT_PING = 1
    HANDSHAKE = 6
    AUTH_SNAPSHOT = 19
    LOGOUT = 20
    STICKER_STORE = 27
    ASSET_GET = 28
    FAVORITE_STICKER = 29
    CONTACT_GET = 32
    CONTACT_PRESENCE = 35
    CHAT_GET = 48
    OPEN_BY_LINK = 57
    SEND_MESSAGE = 64
    ATTACH_TYPING = 65        # "I'm uploading <type> in this chat"
    EDIT_MESSAGE = 67
    PHOTO_UPLOAD_URL = 80     # get URL for photo upload
    VIDEO_UPLOAD_URL = 82
    VIDEO_DOWNLOAD_URL = 83
    AUDIO_UPLOAD_URL = 86     # experimental; not the native voice endpoint
    FILE_UPLOAD_URL = 87      # get URL for file upload
    FILE_DOWNLOAD_URL = 88
    DISPATCH = 128
    READ_MARK = 130
    UPLOAD_READY = 136        # server says an uploaded file/video is processed


@dataclass
class MaxMessage:
    """A validated incoming MAX message.

    ``revision`` is a hash of the content that matters. Two frames for one
    message ID with the same revision are a replay and get dropped; a different
    revision is an edit and is handled as one (audit D05).
    """

    chat_id: Any = None
    sender_id: Any = None
    text: str = ""
    timestamp: Any = None
    message_id: str = ""
    is_self: bool = False
    attaches: list = field(default_factory=list)
    elements: list = field(default_factory=list)
    link: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)
    revision: str = ""

    @property
    def chat_key(self) -> str:
        return chat_key(self.chat_id)

    def to_payload(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "sender_id": self.sender_id,
            "text": self.text,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "is_self": self.is_self,
            "attaches": self.attaches,
            "elements": self.elements,
            "link": self.link,
            "revision": self.revision,
        }

    @classmethod
    def from_payload(cls, data: dict) -> MaxMessage:
        return cls(
            chat_id=data.get("chat_id"),
            sender_id=data.get("sender_id"),
            text=data.get("text") or "",
            timestamp=data.get("timestamp"),
            message_id=data.get("message_id") or "",
            is_self=bool(data.get("is_self")),
            attaches=data.get("attaches") or [],
            elements=data.get("elements") or [],
            link=data.get("link") or {},
            revision=data.get("revision") or "",
        )


class MessageValidationError(Exception):
    """A DISPATCH frame we refuse to trust. Quarantined, never fatal."""


def validate_message(payload: dict) -> MaxMessage:
    """Turn a raw DISPATCH payload into a normalised message, or reject it.

    ``str(msg_body.get("id", ""))`` used to turn a JSON ``null`` into the string
    ``"None"``, which then became a shared dedup key that swallowed every later
    null-ID event (audit D03). IDs are validated before conversion, and an
    unusable one lands in quarantine where it is visible, rather than silently
    poisoning the cache.
    """
    if not isinstance(payload, dict):
        raise MessageValidationError("payload is not an object")
    body = payload.get("message")
    if not isinstance(body, dict):
        raise MessageValidationError("no message object in payload")

    raw_id = body.get("id")
    if raw_id is None:
        raise MessageValidationError("message has no id")
    if isinstance(raw_id, bool) or not isinstance(raw_id, (int, str)):
        raise MessageValidationError(f"message id has type {type(raw_id).__name__}")
    message_id = str(raw_id).strip()
    if not message_id:
        raise MessageValidationError("message id is empty")

    chat_id = payload.get("chatId", body.get("chatId"))
    if chat_id is None:
        raise MessageValidationError("message has no chatId")

    text = body.get("text")
    if text is None:
        text = ""
    elif not isinstance(text, str):
        text = str(text)

    attaches = body.get("attaches")
    if not isinstance(attaches, list):
        attaches = []
    attaches = [a for a in attaches if isinstance(a, dict)]

    elements = body.get("elements")
    if not isinstance(elements, list):
        elements = []
    elements = [e for e in elements if isinstance(e, dict)]

    link = body.get("link")
    if not isinstance(link, dict):
        link = {}

    msg = MaxMessage(
        chat_id=chat_id,
        sender_id=body.get("sender"),
        text=text,
        timestamp=body.get("time"),
        message_id=message_id,
        attaches=attaches,
        elements=elements,
        link=link,
        raw=payload,
    )
    # The revision covers what a user would call "the message": its text, its
    # attachments, its formatting and what it quotes. Server bookkeeping such as
    # read counters is left out so it does not manufacture phantom edits.
    msg.revision = revision_hash({
        "text": msg.text,
        "attaches": msg.attaches,
        "elements": msg.elements,
        "link": msg.link,
        "sender": msg.sender_id,
    })
    return msg


@dataclass
class _Pending:
    future: asyncio.Future
    opcode: int


class _Connection:
    """One WebSocket generation and everything scoped to it."""

    _generation_counter = 0

    def __init__(self, ws: aiohttp.ClientWebSocketResponse, write_timeout: float):
        _Connection._generation_counter += 1
        self.generation = _Connection._generation_counter
        self.ws = ws
        self.write_timeout = write_timeout
        self.seq = 0
        self.pending: dict[int, _Pending] = {}
        self.file_pending: dict[int, asyncio.Future] = {}
        self.ping_replies = 0
        self.last_rx = time.monotonic()
        self.closed = False

    def next_seq(self) -> int:
        seq = self.seq
        self.seq += 1
        return seq

    async def send(self, opcode: int, seq: int, payload: dict) -> None:
        pkt = {"ver": 11, "cmd": 0, "seq": seq, "opcode": int(opcode),
               "payload": payload}
        raw = json.dumps(pkt, ensure_ascii=False)
        log.debug(">>> SEND op=%d seq=%d len=%d", opcode, seq, len(raw))
        # A wedged socket write is one of the ways this bridge used to freeze:
        # the coroutine never returned and nothing was watching it (audit R05).
        await asyncio.wait_for(self.ws.send_str(raw), timeout=self.write_timeout)

    def fail_all(self, exc: BaseException) -> None:
        """Resolve every future this connection owns with an exception."""
        self.closed = True
        for pending in list(self.pending.values()):
            if not pending.future.done():
                pending.future.set_exception(exc)
        self.pending.clear()
        for fut in list(self.file_pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self.file_pending.clear()


class MaxClient:
    WS_URL = "wss://ws-api.oneme.ru/websocket"

    HEARTBEAT_SEC = 30
    # Protocol-level ping/pong, handled by aiohttp. Independent of MAX's own
    # opcode 1, and the only liveness check that does not depend on the server
    # implementing an application heartbeat.
    WS_PING_SEC = 25
    WRITE_TIMEOUT_SEC = 20
    CONNECT_TIMEOUT_SEC = 20
    HANDSHAKE_TIMEOUT_SEC = 20
    AUTH_TIMEOUT_SEC = 45
    RPC_TIMEOUT_SEC = 15
    CLOSE_TIMEOUT_SEC = 10
    # Silence that means a dead link once MAX is known to answer our pings.
    LIVENESS_TIMEOUT_SEC = 120
    # Fallback for a server build that ignores opcode 1. Generous, because on
    # such a build silence may simply mean an idle account — but not infinite,
    # which is what the old "watchdog stays disabled forever" amounted to
    # (audit R05).
    SILENT_SERVER_TIMEOUT_SEC = 900
    RECONNECT_MIN_SEC = 2
    RECONNECT_MAX_SEC = 300
    # How long a connection must hold before we consider the link healthy again
    # and reset the backoff. Resetting on connect alone makes a flapping link
    # reconnect as fast as a healthy one.
    STABLE_AFTER_SEC = 120
    ALIVE_REPORT_SEC = 600

    def __init__(self, token: str, device_id: str, chat_ids: str | None = None,
                 debug: bool = False, *,
                 health: Health | None = None,
                 fetcher: SafeFetcher | None = None,
                 spool: MediaSpool | None = None,
                 enable_audio_probing: bool = False):
        self.token = token
        self.device_id = device_id
        self.debug = debug
        self.health = health or Health()
        self._fetcher = fetcher
        self._spool = spool
        self._enable_audio_probing = enable_audio_probing

        self._conn: _Connection | None = None
        self._session: aiohttp.ClientSession | None = None
        self._my_id: Any = None

        self._on_ready_cb: Callable[[dict], Awaitable[None]] | None = None
        self._on_message_cb: Callable[[MaxMessage], Awaitable[None]] | None = None
        self._on_disconnect_cb: Callable[[str], Awaitable[None]] | None = None
        self._on_quarantine_cb: Callable[[str, dict], Awaitable[None]] | None = None

        self._ready_event = asyncio.Event()
        self._stopping = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()
        self._ready_task: asyncio.Task | None = None
        self._last_alive_report = time.monotonic()
        self._connected_at: float | None = None
        self._auth_failure: str | None = None

        # Inbound routing filter. Named for what it is: it never restricted
        # sending, and calling it an access boundary was the misleading part
        # (audit S06).
        self.inbound_chat_filter: list[str] = []
        if chat_ids:
            self.inbound_chat_filter = [
                chat_key(c.strip()) for c in str(chat_ids).split(",") if c.strip()
            ]

    # ── decorator API ──────────────────────────────────────────────

    def on_ready(self, func):
        self._on_ready_cb = func
        return func

    def on_message(self, func):
        self._on_message_cb = func
        return func

    def on_disconnect(self, func):
        self._on_disconnect_cb = func
        return func

    def on_quarantine(self, func):
        self._on_quarantine_cb = func
        return func

    # ── state ──────────────────────────────────────────────────────

    @property
    def state(self) -> LinkState:
        return self.health.state

    @property
    def ready(self) -> bool:
        return self.health.ready and self._conn is not None and not self._conn.closed

    @property
    def my_id(self) -> Any:
        return self._my_id

    async def wait_ready(self, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout)
            return True
        except TimeoutError:
            return False

    def _set_state(self, state: LinkState, error: str | None = None) -> None:
        if state is not self.health.state:
            log.info("MAX link state: %s → %s%s", self.health.state.value,
                     state.value, f" ({error})" if error else "")
        self.health.set_state(state, error)
        if state.is_ready:
            self._ready_event.set()
        else:
            self._ready_event.clear()

    # ── RPC ────────────────────────────────────────────────────────

    async def cmd(self, opcode: int, payload: dict,
                  timeout: float | None = None, *,
                  require_ready: bool = True) -> dict:
        """Send a request and await its reply.

        Raises rather than returning a sentinel: :class:`NotReady` when there is
        no authorised connection to send on, :class:`ConnectionLost` when the
        link dies mid-flight, :class:`MaxRpcError` on ``cmd=3``, and
        ``asyncio.TimeoutError`` when the reply never comes. The old ``{}``-on-
        failure contract meant every caller had to guess which of those happened
        — and most did not try.
        """
        conn = self._conn
        if conn is None or conn.closed or conn.ws.closed:
            raise NotReady(f"no MAX connection for opcode {opcode}")
        if require_ready and not self.health.ready:
            raise NotReady(
                f"MAX link is {self.health.state.value}, not ready for opcode {opcode}")

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict] = loop.create_future()
        seq = conn.next_seq()
        conn.pending[seq] = _Pending(fut, int(opcode))
        try:
            await conn.send(opcode, seq, payload)
        except BaseException:
            conn.pending.pop(seq, None)
            raise
        try:
            return await asyncio.wait_for(fut, timeout or self.RPC_TIMEOUT_SEC)
        except TimeoutError:
            self.health.counters.bump("rpc_timeouts")
            log.warning("RPC timeout: opcode=%d seq=%d", opcode, seq)
            raise
        finally:
            # Only ever removes our own entry on our own connection.
            entry = conn.pending.get(seq)
            if entry is not None and entry.future is fut:
                conn.pending.pop(seq, None)

    async def try_cmd(self, opcode: int, payload: dict,
                      timeout: float | None = None) -> dict | None:
        """``cmd`` for callers that treat every failure the same way."""
        try:
            return await self.cmd(opcode, payload, timeout)
        except (TimeoutError, NotReady, ConnectionLost, MaxRpcError) as exc:
            log.info("RPC opcode=%s failed: %s", opcode, describe_exception(exc))
            return None

    # ── connection supervisor ──────────────────────────────────────

    async def run(self) -> None:
        """Single supervisor for the whole connection lifecycle.

        Only this coroutine opens sockets: several tasks each noticing a problem
        and each dialling out is how you end up with parallel connections
        fighting over the same account (audit 14.4).
        """
        if self.debug:
            os.makedirs(DEBUG_DIR, exist_ok=True)

        backoff = self.RECONNECT_MIN_SEC
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.CONNECT_TIMEOUT_SEC)
        async with aiohttp.ClientSession(headers=_BROWSER_HEADERS,
                                         timeout=timeout) as session:
            self._session = session
            while not self._stopping.is_set():
                reason = "unknown"
                self._connected_at = None
                try:
                    reason = await self._run_once(session)
                except AuthFailed as exc:
                    reason = describe_exception(exc)
                    self._auth_failure = str(exc)
                    self._set_state(LinkState.AUTH_REQUIRED, reason)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reason = describe_exception(exc)
                    log.warning("MAX connection error: %s", reason)
                finally:
                    await self._teardown(reason)

                if self._stopping.is_set():
                    break

                await self._notify_disconnect(reason)

                held = (time.monotonic() - self._connected_at
                        if self._connected_at else 0.0)
                if held >= self.STABLE_AFTER_SEC:
                    backoff = self.RECONNECT_MIN_SEC

                if self.health.state is LinkState.AUTH_REQUIRED:
                    # A confirmed credential rejection will not fix itself. Keep
                    # trying, but slowly, and stop pretending it is a network
                    # blip (audit 14.5).
                    delay = self.RECONNECT_MAX_SEC
                else:
                    self._set_state(LinkState.RECONNECT_BACKOFF, reason)
                    delay = min(backoff, self.RECONNECT_MAX_SEC)
                    backoff = min(backoff * 2, self.RECONNECT_MAX_SEC)
                # Jitter keeps a fleet — or a restart loop — from synchronising.
                delay *= 0.7 + random.random() * 0.6
                log.info("Reconnecting to MAX in %.0fs (state=%s)",
                         delay, self.health.state.value)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                    break
                except TimeoutError:
                    pass

        self._set_state(LinkState.STOPPED)

    async def _run_once(self, session: aiohttp.ClientSession) -> str:
        """One connection attempt. Returns the reason it ended."""
        self._set_state(LinkState.CONNECTING)
        log.info("Connecting to %s ...", self.WS_URL)
        ws = await asyncio.wait_for(
            session.ws_connect(
                self.WS_URL, headers=_WS_HEADERS,
                heartbeat=self.WS_PING_SEC,
                timeout=aiohttp.ClientWSTimeout(ws_close=self.CLOSE_TIMEOUT_SEC),
            ),
            timeout=self.CONNECT_TIMEOUT_SEC,
        )
        async with ws:
            conn = _Connection(ws, self.WRITE_TIMEOUT_SEC)
            self._conn = conn
            self._connected_at = time.monotonic()
            self.health.counters.bump("reconnects")

            heartbeat = self._spawn(self._heartbeat_loop(conn), "heartbeat")
            watchdog = self._spawn(self._watchdog_loop(conn), "watchdog")
            try:
                self._set_state(LinkState.HANDSHAKING)
                await self._handshake(conn)
                self._set_state(LinkState.AUTHENTICATING)
                await self._authenticate(conn)
                return await self._read_loop(conn)
            finally:
                for task in (heartbeat, watchdog):
                    task.cancel()
                await asyncio.gather(heartbeat, watchdog, return_exceptions=True)

    async def _handshake(self, conn: _Connection) -> None:
        payload = {
            "deviceId": self.device_id,
            "userAgent": {"deviceType": "WEB", "deviceName": "Chrome 131.0.0.0"},
            "appVersion": "26.2.2",
        }
        seq = conn.next_seq()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict] = loop.create_future()
        conn.pending[seq] = _Pending(fut, int(OpCode.HANDSHAKE))
        await conn.send(OpCode.HANDSHAKE, seq, payload)
        try:
            await asyncio.wait_for(
                self._pump_until(conn, fut), timeout=self.HANDSHAKE_TIMEOUT_SEC)
        except TimeoutError:
            raise ConnectionLost("handshake did not complete in time") from None
        finally:
            conn.pending.pop(seq, None)
        log.info("Handshake OK")

    async def _authenticate(self, conn: _Connection) -> None:
        payload = {"chatsCount": 40, "interactive": True, "token": self.token}
        seq = conn.next_seq()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict] = loop.create_future()
        conn.pending[seq] = _Pending(fut, int(OpCode.AUTH_SNAPSHOT))
        await conn.send(OpCode.AUTH_SNAPSHOT, seq, payload)
        try:
            snapshot = await asyncio.wait_for(
                self._pump_until(conn, fut), timeout=self.AUTH_TIMEOUT_SEC)
        except TimeoutError:
            # Silence is not proof the token is bad — say what we actually know.
            raise ConnectionLost(
                f"authorisation did not complete within {self.AUTH_TIMEOUT_SEC}s"
            ) from None
        except MaxRpcError as exc:
            if self._is_auth_rejection(exc.payload):
                raise AuthFailed(
                    f"MAX rejected the token: {exc.describe()}") from exc
            raise ConnectionLost(f"auth failed: {exc.describe()}") from exc
        finally:
            conn.pending.pop(seq, None)

        if not isinstance(snapshot, dict) or not snapshot.get("profile"):
            raise ConnectionLost("AUTH_SNAPSHOT carried no profile — not ready")

        self._my_id = (snapshot.get("profile") or {}).get("id")
        self._auth_failure = None
        self._set_state(LinkState.READY)
        log.info("Authorized. my_id=%s", self._my_id)
        if self.debug:
            self._dump_json("snapshot.json", snapshot)

        if self._on_ready_cb:
            # Fire-and-forget: on_ready issues its own RPCs, whose replies only
            # this read loop can deliver. Awaiting it inline deadlocks.
            if self._ready_task and not self._ready_task.done():
                self._ready_task.cancel()
                try:
                    await self._ready_task
                except (asyncio.CancelledError, Exception):
                    pass
            self._ready_task = self._spawn(self._on_ready_cb(snapshot), "on_ready")

    @staticmethod
    def _is_auth_rejection(payload: dict) -> bool:
        blob = json.dumps(payload or {}, ensure_ascii=False).lower()
        return any(marker in blob for marker in _AUTH_REJECTED_MARKERS)

    async def _pump_until(self, conn: _Connection,
                          fut: asyncio.Future) -> dict:
        """Read frames until ``fut`` resolves.

        Handshake and auth happen before the main read loop exists, so somebody
        has to drive the socket during them — doing it here is what gives those
        two phases a real deadline instead of an open-ended wait.
        """
        while not fut.done():
            msg = await conn.ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_frame(conn, msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                              aiohttp.WSMsgType.ERROR):
                raise ConnectionLost(f"socket closed during setup: {msg.type.name}")
        return await fut

    async def _read_loop(self, conn: _Connection) -> str:
        async for msg in conn.ws:
            if self._stopping.is_set():
                return "shutdown"
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_frame(conn, msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                return f"socket {msg.type.name.lower()}"
        return "socket ended"

    async def _handle_frame(self, conn: _Connection, raw: str) -> None:
        """Parse one frame. A bad frame is quarantined, never fatal.

        A malformed payload used to raise out of the handler and tear down the
        connection, and on a poison event that is an endless reconnect loop
        (audit R12). The exception is a frame that ends the *session* rather
        than being merely unreadable — see the re-raise below.
        """
        conn.last_rx = time.monotonic()
        self.health.note_frame()
        try:
            data = json.loads(raw)
        except Exception:
            log.warning("Dropping unparseable frame (%d bytes)", len(raw))
            self.health.counters.bump("quarantined")
            return
        if not isinstance(data, dict):
            log.warning("Dropping non-object frame of type %s", type(data).__name__)
            self.health.counters.bump("quarantined")
            return
        try:
            await self._dispatch(conn, data)
        except asyncio.CancelledError:
            raise
        except (AuthFailed, ConnectionLost):
            # Lifecycle signals, not bad frames. A server-sent LOGOUT means the
            # session is over, and only run() can act on that by entering
            # auth_required; swallowing it here left the bridge sitting in
            # `ready` on a socket MAX had already disowned (audit R04, §14.5).
            raise
        except Exception as exc:
            log.exception("Error handling opcode=%s: %s",
                          data.get("opcode"), describe_exception(exc))
            self.health.counters.bump("quarantined")

    async def _dispatch(self, conn: _Connection, data: dict) -> None:
        op = data.get("opcode")
        cmd = data.get("cmd")
        seq = data.get("seq")
        payload = data.get("payload")
        if not isinstance(payload, dict):
            payload = {}

        if cmd in (1, 3) and isinstance(seq, int):
            pending = conn.pending.pop(seq, None)
            if pending is not None:
                if pending.opcode != op:
                    log.warning("Reply opcode %s does not match request opcode %s"
                                " for seq %s — ignoring", op, pending.opcode, seq)
                elif not pending.future.done():
                    if cmd == 1:
                        pending.future.set_result(payload)
                    else:
                        pending.future.set_exception(MaxRpcError(int(op or 0), payload))
                return
            if cmd == 3:
                log.warning("Unsolicited error frame op=%s: %s", op,
                            json.dumps(payload, ensure_ascii=False)[:300])
                return

        if op == OpCode.DISPATCH:
            await self._handle_dispatch(payload)
        elif op == OpCode.UPLOAD_READY:
            self._handle_upload_ready(conn, payload)
        elif op == OpCode.HEARTBEAT_PING:
            conn.ping_replies += 1
        elif op == OpCode.LOGOUT:
            raise AuthFailed("MAX sent LOGOUT — the session was terminated")
        elif op == OpCode.READ_MARK:
            log.debug("read-mark frame")
        else:
            log.debug("<<< EVENT op=%s cmd=%s", op, cmd)

    async def _handle_dispatch(self, payload: dict) -> None:
        self.health.counters.bump("dispatches")
        if self._on_message_cb is None:
            return
        try:
            msg = validate_message(payload)
        except MessageValidationError as exc:
            log.warning("Quarantined DISPATCH: %s", exc)
            self.health.counters.bump("quarantined")
            if self._on_quarantine_cb:
                await self._on_quarantine_cb(str(exc), payload)
            return

        if self.inbound_chat_filter and msg.chat_key not in self.inbound_chat_filter:
            return

        if self._my_id is not None and msg.sender_id is not None:
            # Compare normalised IDs: MAX is not consistent about int vs str,
            # and a type mismatch silently defeated the echo guard (audit D07).
            msg.is_self = chat_key(msg.sender_id) == chat_key(self._my_id)

        lag = self._lag_seconds(msg)
        log.info("Accepted message id=%s chat=%s attaches=%d lag=%s",
                 msg.message_id, msg.chat_key, len(msg.attaches),
                 f"{lag:.1f}s" if lag is not None else "?")
        self.health.counters.bump("accepted")
        await self._on_message_cb(msg)

    def _handle_upload_ready(self, conn: _Connection, payload: dict) -> None:
        raw_id = payload.get("fileId")
        try:
            file_id = int(raw_id)
        except (TypeError, ValueError):
            log.debug("UPLOAD_READY without a usable fileId: %s", payload)
            return
        fut = conn.file_pending.pop(file_id, None)
        if fut and not fut.done():
            fut.set_result(payload)

    # ── liveness ───────────────────────────────────────────────────

    async def _heartbeat_loop(self, conn: _Connection) -> None:
        """Send the application-level ping. Failing to send is a link failure."""
        while not conn.closed and not conn.ws.closed:
            await asyncio.sleep(self.HEARTBEAT_SEC)
            if conn.closed or conn.ws.closed:
                return
            try:
                seq = conn.next_seq()
                await conn.send(OpCode.HEARTBEAT_PING, seq, {"interactive": False})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Previously this just ended the heartbeat task and left the
                # bridge thinking it was connected (audit R05).
                log.warning("Heartbeat failed (%s) — closing the link",
                            describe_exception(exc))
                await self._force_close(conn, "heartbeat write failed")
                return

    async def _watchdog_loop(self, conn: _Connection) -> None:
        """Independent stall detector.

        Runs on its own task precisely so that a wedged heartbeat write cannot
        also disable the thing meant to notice it.
        """
        while not conn.closed and not conn.ws.closed:
            await asyncio.sleep(5)
            now = time.monotonic()
            silence = now - conn.last_rx
            state = self.health.state

            if state in (LinkState.HANDSHAKING, LinkState.AUTHENTICATING):
                # Those phases carry their own wait_for deadlines; nothing to do
                # here beyond letting them run.
                continue

            if now - self._last_alive_report >= self.ALIVE_REPORT_SEC:
                self._last_alive_report = now
                log.info("Link alive: state=%s last frame %.0fs ago, "
                         "%d dispatch(es), %d ping replies",
                         state.value, silence,
                         self.health.counters.dispatches, conn.ping_replies)

            limit = (self.LIVENESS_TIMEOUT_SEC if conn.ping_replies
                     else self.SILENT_SERVER_TIMEOUT_SEC)
            if silence >= limit:
                reason = (f"no frames for {silence:.0f}s "
                          f"(ping replies: {conn.ping_replies})")
                log.warning("Liveness watchdog: %s — forcing reconnect", reason)
                await self._force_close(conn, reason)
                return

    async def _force_close(self, conn: _Connection, reason: str) -> None:
        conn.closed = True
        try:
            await asyncio.wait_for(conn.ws.close(), timeout=self.CLOSE_TIMEOUT_SEC)
        except Exception:
            log.debug("Closing a stalled socket did not finish cleanly (%s)", reason)

    async def _teardown(self, reason: str) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            conn.fail_all(ConnectionLost(f"MAX link lost: {reason}"))
        if self.health.state not in (LinkState.AUTH_REQUIRED, LinkState.STOPPING,
                                     LinkState.STOPPED):
            self._set_state(LinkState.DISCONNECTED, reason)

    async def _notify_disconnect(self, reason: str) -> None:
        if not self._on_disconnect_cb:
            return
        try:
            # The callback must not block reconnection — a Telegram outage used
            # to hold the MAX link down while a status message retried
            # (audit R10). Status delivery is queued on the listener's side.
            await self._on_disconnect_cb(reason)
        except Exception:
            log.exception("on_disconnect callback error")

    # ── shutdown ───────────────────────────────────────────────────

    async def stop(self) -> None:
        """Stop reconnecting and let in-flight work finish or fail cleanly."""
        self._stopping.set()
        self._set_state(LinkState.STOPPING)
        conn = self._conn
        if conn is not None:
            await self._force_close(conn, "shutdown")
        if self._ready_task and not self._ready_task.done():
            self._ready_task.cancel()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._set_state(LinkState.STOPPED)

    def _spawn(self, coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=f"max:{name}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(_log_task_exception)
        return task

    # ── high-level API ─────────────────────────────────────────────

    async def fetch_contacts(self, contact_ids: list[int]) -> dict:
        if not contact_ids:
            return {}
        return await self.cmd(OpCode.CONTACT_GET, {"contactIds": list(contact_ids)})

    async def fetch_chat(self, chat_id: Any) -> dict:
        """Ask for one chat's metadata.

        The snapshot only carries the first page of chats, so a chat that is not
        in it had no title and got named after whoever spoke first (audit T07).
        """
        return await self.cmd(OpCode.CHAT_GET, {"chatIds": [chat_id]})

    async def send_message(self, chat_id, text: str = "", elements=None,
                           attaches=None, *, cid: int | None = None,
                           link: dict | None = None) -> dict:
        """Send one message to MAX.

        ``cid`` is stable per logical operation so a retry of the *same* send
        reuses it. Whether MAX deduplicates on it is unverified, so this does not
        promise exactly-once — it just stops us from inventing a fresh identity
        for every attempt (audit D06).
        """
        message = {
            "text": text,
            "cid": cid if cid is not None else self.new_cid(),
            "elements": list(elements or []),
        }
        if attaches:
            message["attaches"] = list(attaches)
        if link:
            # Unconfirmed wire format — see TelegramToMaxHandler._reply_link.
            # Nothing sets this unless MAX_REPLY_LINK is on.
            message["link"] = dict(link)
        return await self.cmd(
            OpCode.SEND_MESSAGE,
            {"chatId": chat_id, "message": message, "notify": True},
            timeout=self.RPC_TIMEOUT_SEC * 2,
        )

    @staticmethod
    def new_cid() -> int:
        return int(time.time() * 1000) * 1000 + random.randint(0, 999)

    async def open_by_link(self, link: str) -> dict:
        """Resolve a ``max.ru/join/<token>`` invite.

        ``/u/<token>`` share links live in a different namespace and opcode 57
        answers ``not.found`` for them; that is a known gap, not a bug to retry.
        """
        return await self.cmd(OpCode.OPEN_BY_LINK, {"link": link})

    # ── media ──────────────────────────────────────────────────────

    def _require_fetcher(self) -> SafeFetcher:
        if self._fetcher is None:
            raise RuntimeError("MaxClient was built without a media fetcher")
        return self._fetcher

    async def download_media(self, url: str, *, filename: str = "file",
                             max_bytes: int | None = None) -> SpoolFile:
        """Stream a media URL to the spool, enforcing the fetch policy."""
        spooled = await self._require_fetcher().download(
            url, filename=filename, max_bytes=max_bytes)
        self.health.counters.bump("downloads")
        self.health.counters.bump("download_bytes", spooled.size)
        log.info("Downloaded %s (%d bytes)", safe_url(url), spooled.size)
        return spooled

    async def download_small(self, url: str, *, limit: int = 8 * 1024 * 1024
                             ) -> bytes | None:
        """Fetch something small (an avatar) straight into memory."""
        try:
            spooled = await self.download_media(url, max_bytes=limit)
        except Exception as exc:
            log.info("Small download failed for %s: %s", safe_url(url),
                     describe_exception(exc))
            return None
        try:
            return spooled.read_bytes()
        finally:
            spooled.release()

    async def resolve_file_url(self, file_id: Any, chat_id: Any,
                               message_id: str) -> str | None:
        """Ask MAX for a download URL for a FILE attachment (opcode 88)."""
        try:
            fid = int(file_id)
        except (TypeError, ValueError):
            return None
        resp = await self.try_cmd(OpCode.FILE_DOWNLOAD_URL, {
            "fileId": fid, "chatId": chat_id, "messageId": str(message_id),
        })
        url = (resp or {}).get("url")
        return url if isinstance(url, str) and url.startswith("http") else None

    async def resolve_video_url(self, video_id: Any, chat_id: Any,
                                message_id: str) -> str | None:
        """Ask MAX for the real video URL rather than settling for a thumbnail."""
        try:
            vid = int(video_id)
        except (TypeError, ValueError):
            return None
        resp = await self.try_cmd(OpCode.VIDEO_DOWNLOAD_URL, {
            "videoId": vid, "chatId": chat_id, "messageId": str(message_id),
        })
        if not resp:
            return None
        for key in ("url", "mp4_1080", "mp4_720", "mp4_480", "mp4_360"):
            url = resp.get(key)
            if isinstance(url, str) and url.startswith("http"):
                return url
        return None

    async def resolve_audio_url(self, audio_id: Any, chat_id: Any,
                                message_id: str, token: str | None = None
                                ) -> str | None:
        """Best-effort URL for the ``_type=UNSUPPORTED`` voice shape.

        The speculative CDN-pattern probing that used to run here made a user's
        chat wait through four sequential HTTP attempts before anything was
        forwarded, and accepted any non-HTML 200 as "audio" (audit M09/T08).
        It is now off unless ``MAX_AUDIO_PROBING=true`` says otherwise; the
        supported path is opcode 88, which is a documented download endpoint.
        """
        url = await self.resolve_file_url(audio_id, chat_id, message_id)
        if url:
            return url
        if not self._enable_audio_probing or not token:
            return None
        log.info("Audio probing enabled — trying CDN URL patterns")
        for candidate in (f"https://i.oneme.ru/i?r={token}",
                          f"https://i.oneme.ru/a?r={token}"):
            try:
                spooled = await self._require_fetcher().download(
                    candidate, filename="probe", max_bytes=1024 * 1024)
            except Exception:
                continue
            content_type = spooled.content_type
            spooled.release()
            from app.net import looks_like_media
            if looks_like_media(content_type):
                return candidate
        return None

    async def upload_photo(self, source: bytes | SpoolFile, chat_id=None,
                           filename: str = "image.jpg",
                           mimetype: str = "image/jpeg") -> dict | None:
        resp = await self.try_cmd(OpCode.PHOTO_UPLOAD_URL, {"count": 1})
        url = (resp or {}).get("url")
        if not url:
            log.error("Photo upload URL request returned no url")
            return None

        if chat_id is not None:
            await self.try_cmd(OpCode.ATTACH_TYPING,
                               {"chatId": chat_id, "type": "PHOTO"})

        body = await self._http_upload(url, source, filename, mimetype,
                                       expect_json=True)
        if not isinstance(body, dict):
            return None
        photos = body.get("photos") or {}
        first = next(iter(photos.values()), None) if photos else None
        token = first.get("token") if isinstance(first, dict) else None
        if not token:
            log.error("Photo upload response carried no token")
            return None
        return {"_type": "PHOTO", "photoToken": token}

    async def upload_file(self, source: bytes | SpoolFile, chat_id=None,
                          filename: str = "file.bin",
                          mimetype: str = "application/octet-stream",
                          attach_type: str = "FILE",
                          timeout: float = 120.0) -> dict | None:
        """Upload a file and wait for the server to finish ingesting it.

        The pending future is registered before the POST (an UPLOAD_READY can
        beat our own coroutine back) and removed in ``finally`` on every path,
        including cancellation — the old code leaked entries on several branches
        (audit R11).
        """
        conn = self._conn
        if conn is None or conn.closed:
            raise NotReady("cannot upload without a MAX connection")

        resp = await self.try_cmd(OpCode.FILE_UPLOAD_URL, {"count": 1})
        info_list = (resp or {}).get("info") or []
        info = info_list[0] if info_list else {}
        url = info.get("url")
        raw_file_id = info.get("fileId")
        if not url or raw_file_id is None:
            log.error("File upload URL request returned no url/fileId")
            return None
        try:
            file_id = int(raw_file_id)
        except (TypeError, ValueError):
            log.error("File upload returned a non-numeric fileId")
            return None

        if chat_id is not None:
            await self.try_cmd(OpCode.ATTACH_TYPING,
                               {"chatId": chat_id, "type": attach_type})

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        conn.file_pending[file_id] = fut
        try:
            ok = await self._http_upload(url, source, filename, mimetype,
                                         expect_json=False)
            if not ok:
                return None
            try:
                await asyncio.wait_for(fut, timeout=timeout)
            except TimeoutError:
                log.warning("Server did not confirm upload fileId=%s in %.0fs",
                            file_id, timeout)
                return None
            except ConnectionLost:
                log.warning("Link dropped while waiting for upload confirmation")
                return None
            return {"_type": attach_type, "fileId": file_id}
        finally:
            if conn.file_pending.get(file_id) is fut:
                conn.file_pending.pop(file_id, None)

    async def upload_audio(self, source: bytes | SpoolFile, chat_id=None,
                           filename: str = "voice.ogg",
                           mimetype: str = "audio/ogg",
                           timeout: float = 120.0) -> dict | None:
        """Voice goes out as a FILE attachment.

        MAX keeps audio in a separate namespace and rejects a file-upload
        ``fileId`` there. Until the native endpoint is captured from the web
        client, a Telegram voice message arrives in MAX as an ``.ogg`` file, and
        the bridge says so rather than pretending otherwise.
        """
        return await self.upload_file(source, chat_id=chat_id, filename=filename,
                                      mimetype=mimetype, attach_type="FILE",
                                      timeout=timeout)

    async def _http_upload(self, url: str, source: bytes | SpoolFile,
                           filename: str, mimetype: str, expect_json: bool):
        from app.net import validate_url

        fetcher = self._require_fetcher()
        await validate_url(url, fetcher.policy)
        session = self._session
        if session is None or session.closed:
            log.error("No HTTP session available for upload")
            return None
        handle = None
        try:
            form = aiohttp.FormData()
            if isinstance(source, SpoolFile):
                handle = source.open()
                form.add_field("file", handle, filename=filename,
                               content_type=mimetype)
            else:
                form.add_field("file", source, filename=filename,
                               content_type=mimetype)
            async with session.post(
                url, headers=_HTTP_HEADERS, data=form,
                timeout=aiohttp.ClientTimeout(total=300),
            ) as resp:
                if resp.status != 200:
                    log.error("Upload POST to %s failed: HTTP %d",
                              safe_url(url), resp.status)
                    return None
                if expect_json:
                    try:
                        return await resp.json(content_type=None)
                    except Exception:
                        log.error("Upload response from %s was not JSON",
                                  safe_url(url))
                        return None
                await resp.read()
                return True
        except Exception as exc:
            log.error("Upload POST to %s failed: %s", safe_url(url),
                      describe_exception(exc))
            return None
        finally:
            if handle is not None:
                handle.close()

    # ── helpers ────────────────────────────────────────────────────

    @staticmethod
    def _lag_seconds(msg: MaxMessage) -> float | None:
        """Seconds between MAX stamping the message and us picking it up.

        Worth reading with suspicion: it mixes MAX's clock with ours, so a large
        value is a hint, not proof that the delay happened upstream.
        """
        try:
            return time.time() - float(msg.timestamp) / 1000.0
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _dump_json(filename: str, data: dict) -> None:
        path = os.path.join(DEBUG_DIR, filename)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log.info("Dumped %s", path)
        except Exception:
            log.exception("Failed to dump %s", path)
