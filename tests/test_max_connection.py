"""End-to-end connection behaviour against a real local WebSocket server.

These are the cases unit tests with mocks cannot honestly cover: a socket that
answers late, a server that never answers at all, a connection that dies with a
request in flight. The audit asked for exactly this (§11, item 5) because every
freeze it found lived in that seam.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web

from app.errors import ConnectionLost
from app.health import Health, LinkState
from app.max_client import MaxClient, OpCode


class FakeMaxServer:
    """A scriptable stand-in for ws-api.oneme.ru."""

    def __init__(self, *, answer_handshake=True, auth_reply="ok",
                 auth_delay=0.0):
        self.answer_handshake = answer_handshake
        self.auth_reply = auth_reply          # "ok" | "reject" | "error" | "silent"
        self.auth_delay = auth_delay
        self.received: list[dict] = []
        self.sockets: list[web.WebSocketResponse] = []
        self.authorized = asyncio.Event()
        self._runner: web.AppRunner | None = None
        self.url = ""

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/websocket", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}/websocket"
        return self.url

    async def stop(self) -> None:
        for ws in list(self.sockets):
            if not ws.closed:
                await ws.close()
        if self._runner is not None:
            await self._runner.cleanup()

    async def _handle(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.sockets.append(ws)
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            packet = json.loads(msg.data)
            self.received.append(packet)
            await self._respond(ws, packet)
        return ws

    async def _respond(self, ws, packet):
        opcode, seq = packet.get("opcode"), packet.get("seq")
        if opcode == OpCode.HANDSHAKE:
            if self.answer_handshake:
                await self._send(ws, opcode, seq, 1, {"ok": True})
            return
        if opcode == OpCode.AUTH_SNAPSHOT:
            if self.auth_delay:
                await asyncio.sleep(self.auth_delay)
            if self.auth_reply == "ok":
                await self._send(ws, opcode, seq, 1, {
                    "profile": {"id": 555, "names": [{"firstName": "Я"}]},
                    "chats": [],
                })
                self.authorized.set()
            elif self.auth_reply == "reject":
                await self._send(ws, opcode, seq, 3,
                                 {"error": "login.token.invalid"})
            elif self.auth_reply == "error":
                await self._send(ws, opcode, seq, 3, {"error": "internal.error"})
            return
        if opcode == OpCode.HEARTBEAT_PING:
            await self._send(ws, opcode, seq, 1, {})
            return
        if opcode == OpCode.CONTACT_GET:
            await self._send(ws, opcode, seq, 1, {"contacts": []})
            return
        if opcode == OpCode.CHAT_GET:
            await self._send(ws, opcode, seq, 1, {"chats": []})
            return

    @staticmethod
    async def _send(ws, opcode, seq, cmd, payload):
        await ws.send_str(json.dumps({"ver": 11, "cmd": cmd, "seq": seq,
                                      "opcode": opcode, "payload": payload}))

    async def push(self, opcode: int, payload: dict) -> None:
        """Server-initiated frame (a DISPATCH, say)."""
        for ws in self.sockets:
            if not ws.closed:
                await ws.send_str(json.dumps({"ver": 11, "cmd": 0, "seq": 999,
                                              "opcode": opcode,
                                              "payload": payload}))

    async def push_raw(self, raw: str) -> None:
        for ws in self.sockets:
            if not ws.closed:
                await ws.send_str(raw)

    async def drop(self) -> None:
        for ws in list(self.sockets):
            if not ws.closed:
                await ws.close()


def build_client(url: str, **kwargs) -> MaxClient:
    client = MaxClient("token-value", "device-value", health=Health(), **kwargs)
    client.WS_URL = url
    # Everything sped up so the tests exercise behaviour, not patience.
    client.HANDSHAKE_TIMEOUT_SEC = 0.5
    client.AUTH_TIMEOUT_SEC = 0.5
    client.CONNECT_TIMEOUT_SEC = 2
    client.RECONNECT_MIN_SEC = 0.05
    client.RECONNECT_MAX_SEC = 0.2
    client.HEARTBEAT_SEC = 0.1
    client.WS_PING_SEC = 5
    client.RPC_TIMEOUT_SEC = 1
    return client


async def wait_for(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.fixture
async def server():
    srv = FakeMaxServer()
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


class TestHappyPath:
    async def test_connect_handshake_auth_ready(self, server):
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            assert client.state is LinkState.READY
            assert client.my_id == 555
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_dispatch_reaches_the_callback(self, server):
        client = build_client(server.url)
        received: list[str] = []

        @client.on_message
        async def _on_message(msg):
            received.append(msg.message_id)

        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.push(int(OpCode.DISPATCH),
                              {"chatId": -1, "message": {"id": 77, "text": "hi"}})
            assert await wait_for(lambda: received == ["77"])
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_rpc_round_trip(self, server):
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            assert await client.fetch_contacts([1]) == {"contacts": []}
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_a_malformed_frame_does_not_drop_the_link(self, server):
        """A poison frame used to tear down the socket and loop forever (R12)."""
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.push_raw("{ this is not json")
            await server.push_raw('"a bare string"')
            await asyncio.sleep(0.1)
            assert client.state is LinkState.READY
            assert client.health.counters.quarantined == 2
            assert await client.fetch_contacts([1]) == {"contacts": []}
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_an_invalid_dispatch_is_quarantined_without_dropping_the_link(
            self, server):
        client = build_client(server.url)
        quarantined: list[str] = []

        @client.on_message
        async def _on_message(msg):
            pass

        @client.on_quarantine
        async def _on_quarantine(reason, payload):
            quarantined.append(reason)

        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.push(int(OpCode.DISPATCH),
                              {"chatId": -1, "message": {"id": None}})
            assert await wait_for(lambda: bool(quarantined))
            assert client.state is LinkState.READY
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class TestAuthFailures:
    async def test_a_server_logout_enters_auth_required(self, server):
        """MAX ending the session must reach the supervisor, not the log.

        ``_dispatch`` raised ``AuthFailed`` on opcode 20 — and ``_handle_frame``,
        which is the only thing that ever calls it, caught ``Exception`` and
        counted it as a bad frame. The bridge went on reporting ``ready`` on a
        socket MAX had already disowned. The old test called ``_dispatch``
        directly and never saw it (audit R04, §14.5).
        """
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.push(int(OpCode.LOGOUT), {})
            assert await wait_for(
                lambda: client.state is LinkState.AUTH_REQUIRED, timeout=5)
            assert not client.ready
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_a_rejected_token_enters_auth_required(self):
        """Only a confirmed rejection may claim the token is bad (audit 14.5)."""
        srv = FakeMaxServer(auth_reply="reject")
        await srv.start()
        client = build_client(srv.url)
        task = asyncio.create_task(client.run())
        try:
            assert await wait_for(
                lambda: client.state is LinkState.AUTH_REQUIRED, timeout=5)
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await srv.stop()

    async def test_a_server_error_is_not_reported_as_a_bad_token(self):
        srv = FakeMaxServer(auth_reply="error")
        await srv.start()
        client = build_client(srv.url)
        task = asyncio.create_task(client.run())
        try:
            assert await wait_for(
                lambda: client.health.counters.reconnects >= 2, timeout=5)
            assert client.state is not LinkState.AUTH_REQUIRED
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await srv.stop()

    async def test_silence_during_auth_hits_its_deadline(self):
        """Auth had no timeout at all: the bridge hung there indefinitely (R04)."""
        srv = FakeMaxServer(auth_reply="silent")
        await srv.start()
        client = build_client(srv.url)
        task = asyncio.create_task(client.run())
        try:
            assert await wait_for(
                lambda: client.health.counters.reconnects >= 2, timeout=6)
            assert client.state is not LinkState.READY
            assert "authorisation did not complete" in (client.health.last_error or "")
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await srv.stop()

    async def test_silence_during_handshake_hits_its_deadline(self):
        srv = FakeMaxServer(answer_handshake=False)
        await srv.start()
        client = build_client(srv.url)
        task = asyncio.create_task(client.run())
        try:
            assert await wait_for(
                lambda: client.health.counters.reconnects >= 2, timeout=6)
            assert "handshake" in (client.health.last_error or "")
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await srv.stop()


class TestDisconnectBehaviour:
    async def test_a_pending_rpc_gets_connection_lost_not_cancelled(self, server):
        """The R01 regression, end to end."""
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            # CONTACT_PRESENCE is deliberately one the fake server never
            # answers, so the request is still in flight when the socket dies.
            pending = asyncio.create_task(
                client.cmd(OpCode.CONTACT_PRESENCE, {"contactIds": [1]},
                           timeout=5))
            await asyncio.sleep(0.05)
            await server.drop()
            with pytest.raises(ConnectionLost):
                await pending
            assert not pending.cancelled()
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_the_bridge_reconnects_and_becomes_ready_again(self, server):
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.drop()
            assert await wait_for(lambda: client.state is not LinkState.READY,
                                  timeout=3)
            assert await client.wait_ready(timeout=5)
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_the_disconnect_callback_fires_with_a_reason(self, server):
        client = build_client(server.url)
        reasons: list[str] = []

        @client.on_disconnect
        async def _on_disconnect(reason):
            reasons.append(reason)

        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.drop()
            assert await wait_for(lambda: bool(reasons), timeout=3)
            assert reasons[0]
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_a_slow_disconnect_callback_does_not_block_reconnection(
            self, server):
        """A Telegram outage used to hold the MAX link down (audit R10)."""
        client = build_client(server.url)
        released = asyncio.Event()

        @client.on_disconnect
        async def _on_disconnect(reason):
            await asyncio.wait_for(released.wait(), timeout=2)

        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            await server.drop()
            await asyncio.sleep(0.2)
            released.set()
            assert await client.wait_ready(timeout=5)
        finally:
            released.set()
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_stop_ends_the_run_loop(self, server):
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        assert await client.wait_ready(timeout=5)
        await client.stop()
        await asyncio.wait_for(asyncio.shield(task), timeout=5)
        assert client.state is LinkState.STOPPED


class TestHeartbeat:
    async def test_ping_replies_are_observed(self, server):
        client = build_client(server.url)
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            assert await wait_for(
                lambda: any(p.get("opcode") == int(OpCode.HEARTBEAT_PING)
                            for p in server.received), timeout=3)
            assert await wait_for(
                lambda: client._conn is not None and client._conn.ping_replies > 0,
                timeout=3)
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_an_idle_but_healthy_link_is_not_reconnected(self, server):
        """The watchdog must not churn a quiet connection (audit 14.3)."""
        client = build_client(server.url)
        client.LIVENESS_TIMEOUT_SEC = 2
        task = asyncio.create_task(client.run())
        try:
            assert await client.wait_ready(timeout=5)
            reconnects = client.health.counters.reconnects
            await asyncio.sleep(1.0)
            assert client.health.counters.reconnects == reconnects
            assert client.state is LinkState.READY
        finally:
            await client.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
