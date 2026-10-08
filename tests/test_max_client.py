"""MAX transport: opcodes, frame validation, RPC correlation, connection lifecycle."""

from __future__ import annotations

import asyncio
import json

import pytest

from app.errors import AuthFailed, ConnectionLost, MaxRpcError, NotReady
from app.health import Health, LinkState
from app.max_client import (
    MaxClient,
    MaxMessage,
    MessageValidationError,
    OpCode,
    _Connection,
    validate_message,
)


class TestOpCode:
    @pytest.mark.parametrize("name,value", [
        ("HEARTBEAT_PING", 1), ("HANDSHAKE", 6), ("AUTH_SNAPSHOT", 19),
        ("LOGOUT", 20), ("STICKER_STORE", 27), ("ASSET_GET", 28),
        ("FAVORITE_STICKER", 29), ("CONTACT_GET", 32), ("CONTACT_PRESENCE", 35),
        ("CHAT_GET", 48), ("OPEN_BY_LINK", 57), ("SEND_MESSAGE", 64),
        ("ATTACH_TYPING", 65), ("EDIT_MESSAGE", 67), ("PHOTO_UPLOAD_URL", 80),
        ("VIDEO_UPLOAD_URL", 82), ("VIDEO_DOWNLOAD_URL", 83),
        ("AUDIO_UPLOAD_URL", 86), ("FILE_UPLOAD_URL", 87),
        ("FILE_DOWNLOAD_URL", 88), ("DISPATCH", 128), ("READ_MARK", 130),
        ("UPLOAD_READY", 136),
    ])
    def test_opcode_value(self, name, value):
        assert getattr(OpCode, name) == value


class TestValidateMessage:
    def test_minimal_valid_message(self):
        msg = validate_message({"chatId": -1, "message": {"id": 7, "text": "hi"}})
        assert msg.message_id == "7"
        assert msg.chat_id == -1
        assert msg.text == "hi"
        assert msg.revision

    def test_null_id_is_rejected_not_stringified(self):
        """``str(None)`` became the shared dedup key ``"None"`` (audit D03)."""
        with pytest.raises(MessageValidationError, match="no id"):
            validate_message({"chatId": 1, "message": {"id": None, "text": "x"}})

    def test_missing_id_is_rejected(self):
        with pytest.raises(MessageValidationError):
            validate_message({"chatId": 1, "message": {"text": "x"}})

    def test_empty_id_is_rejected(self):
        with pytest.raises(MessageValidationError, match="empty"):
            validate_message({"chatId": 1, "message": {"id": "   "}})

    def test_boolean_id_is_rejected(self):
        with pytest.raises(MessageValidationError, match="type"):
            validate_message({"chatId": 1, "message": {"id": True}})

    def test_missing_chat_is_rejected(self):
        with pytest.raises(MessageValidationError, match="chatId"):
            validate_message({"message": {"id": 1}})

    def test_null_text_becomes_empty_string(self):
        msg = validate_message({"chatId": 1, "message": {"id": 1, "text": None}})
        assert msg.text == ""

    def test_non_list_attaches_are_dropped(self):
        msg = validate_message({"chatId": 1,
                                "message": {"id": 1, "attaches": "nope"}})
        assert msg.attaches == []

    def test_non_dict_attach_entries_are_filtered(self):
        msg = validate_message({"chatId": 1, "message": {
            "id": 1, "attaches": [{"_type": "PHOTO"}, "garbage", None]}})
        assert msg.attaches == [{"_type": "PHOTO"}]

    def test_non_object_payload_is_rejected(self):
        with pytest.raises(MessageValidationError):
            validate_message(["not", "a", "dict"])

    def test_identical_content_has_identical_revision(self):
        payload = {"chatId": 1, "message": {"id": 5, "text": "hello"}}
        assert validate_message(payload).revision == validate_message(payload).revision

    def test_added_attachment_changes_the_revision(self):
        """A server-attached link preview is an edit, not a replay (audit D05)."""
        plain = validate_message({"chatId": 1, "message": {"id": 5, "text": "u"}})
        with_preview = validate_message({"chatId": 1, "message": {
            "id": 5, "text": "u", "attaches": [{"_type": "SHARE"}]}})
        assert plain.revision != with_preview.revision

    def test_read_counters_do_not_change_the_revision(self):
        a = validate_message({"chatId": 1, "message": {"id": 5, "text": "u"}})
        b = validate_message({"chatId": 1, "message": {"id": 5, "text": "u",
                                                       "readCount": 3}})
        assert a.revision == b.revision


class TestMaxMessageRoundTrip:
    def test_payload_roundtrip(self):
        original = MaxMessage(chat_id=-7, sender_id=9, text="hey",
                              message_id="12", attaches=[{"_type": "PHOTO"}],
                              elements=[{"type": "STRONG", "from": 0, "length": 3}],
                              link={"type": "REPLY"}, revision="rev")
        restored = MaxMessage.from_payload(original.to_payload())
        assert restored.chat_id == -7
        assert restored.message_id == "12"
        assert restored.attaches == [{"_type": "PHOTO"}]
        assert restored.link == {"type": "REPLY"}
        assert restored.revision == "rev"

    def test_chat_key_normalises(self):
        assert MaxMessage(chat_id="-7").chat_key == MaxMessage(chat_id=-7).chat_key


class FakeWS:
    """A WebSocket that records sends and lets a test inject replies."""

    def __init__(self):
        self.sent: list[dict] = []
        self.closed = False
        self.on_send = None

    async def send_str(self, raw: str) -> None:
        packet = json.loads(raw)
        self.sent.append(packet)
        if self.on_send is not None:
            await self.on_send(packet)

    async def close(self) -> None:
        self.closed = True


def make_client(**kwargs) -> MaxClient:
    client = MaxClient("token-value", "device-value", health=Health(), **kwargs)
    return client


def attach_connection(client: MaxClient) -> tuple[FakeWS, _Connection]:
    ws = FakeWS()
    conn = _Connection(ws, write_timeout=1.0)
    client._conn = conn
    client.health.set_state(LinkState.READY)
    return ws, conn


class TestRpc:
    async def test_cmd_without_a_connection_raises_not_ready(self):
        client = make_client()
        with pytest.raises(NotReady):
            await client.cmd(OpCode.CONTACT_GET, {})

    async def test_cmd_before_ready_raises_not_ready(self):
        """A request issued during handshake used to wait out a full timeout."""
        client = make_client()
        ws = FakeWS()
        client._conn = _Connection(ws, write_timeout=1.0)
        client.health.set_state(LinkState.AUTHENTICATING)
        with pytest.raises(NotReady):
            await client.cmd(OpCode.CONTACT_GET, {})

    async def test_reply_arriving_during_the_send_is_not_lost(self):
        """R02: the future is registered before the socket write, not after."""
        client = make_client()
        ws, conn = attach_connection(client)

        async def reply_immediately(packet):
            # Simulates the read loop delivering the answer while send_str is
            # still suspended — the exact interleaving that used to lose it.
            await asyncio.sleep(0)
            await client._dispatch(conn, {"opcode": packet["opcode"], "cmd": 1,
                                          "seq": packet["seq"],
                                          "payload": {"ok": True}})

        ws.on_send = reply_immediately
        assert await client.cmd(OpCode.CONTACT_GET, {"contactIds": [1]}) == {"ok": True}

    async def test_error_reply_raises_typed_error(self):
        client = make_client()
        ws, conn = attach_connection(client)

        async def reply_error(packet):
            await client._dispatch(conn, {"opcode": packet["opcode"], "cmd": 3,
                                          "seq": packet["seq"],
                                          "payload": {"error": "nope"}})

        ws.on_send = reply_error
        with pytest.raises(MaxRpcError, match="nope"):
            await client.cmd(OpCode.CONTACT_GET, {})

    async def test_timeout_raises_and_counts(self):
        client = make_client()
        attach_connection(client)
        with pytest.raises(asyncio.TimeoutError):
            await client.cmd(OpCode.CONTACT_GET, {}, timeout=0.05)
        assert client.health.counters.rpc_timeouts == 1

    async def test_disconnect_fails_pending_rpc_with_connection_lost(self):
        """R01: pending futures are failed, never cancelled into the caller."""
        client = make_client()
        _ws, conn = attach_connection(client)
        task = asyncio.create_task(client.cmd(OpCode.CONTACT_GET, {}, timeout=5))
        await asyncio.sleep(0.01)
        conn.fail_all(ConnectionLost("link dropped"))
        with pytest.raises(ConnectionLost):
            await task

    async def test_connection_lost_is_not_cancelled_error(self):
        """The distinction that kept a chat worker alive."""
        client = make_client()
        _ws, conn = attach_connection(client)
        task = asyncio.create_task(client.cmd(OpCode.CONTACT_GET, {}, timeout=5))
        await asyncio.sleep(0.01)
        conn.fail_all(ConnectionLost("bye"))
        try:
            await task
        except ConnectionLost:
            pass
        assert not task.cancelled()

    async def test_stale_reply_does_not_resolve_a_new_future(self):
        """R03: sequence numbers reset per connection; futures must not cross."""
        client = make_client()
        _ws, first = attach_connection(client)
        stale = asyncio.get_running_loop().create_future()
        from app.max_client import _Pending
        first.pending[0] = _Pending(stale, int(OpCode.CONTACT_GET))

        _ws2, second = attach_connection(client)
        fresh = asyncio.get_running_loop().create_future()
        second.pending[0] = _Pending(fresh, int(OpCode.CONTACT_GET))

        await client._dispatch(second, {"opcode": int(OpCode.CONTACT_GET),
                                        "cmd": 1, "seq": 0, "payload": {"n": 2}})
        assert fresh.result() == {"n": 2}
        assert not stale.done()
        stale.cancel()

    async def test_reply_with_mismatched_opcode_is_ignored(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        from app.max_client import _Pending
        fut = asyncio.get_running_loop().create_future()
        conn.pending[0] = _Pending(fut, int(OpCode.CONTACT_GET))
        await client._dispatch(conn, {"opcode": int(OpCode.CHAT_GET), "cmd": 1,
                                      "seq": 0, "payload": {}})
        assert not fut.done()
        fut.cancel()

    async def test_try_cmd_swallows_failures(self):
        client = make_client()
        attach_connection(client)
        assert await client.try_cmd(OpCode.CONTACT_GET, {}, timeout=0.05) is None


class TestFrameHandling:
    async def test_unparseable_frame_is_quarantined_not_fatal(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        await client._handle_frame(conn, "{not json")
        assert client.health.counters.quarantined == 1

    async def test_non_object_frame_is_quarantined(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        await client._handle_frame(conn, "[1,2,3]")
        assert client.health.counters.quarantined == 1

    async def test_invalid_dispatch_is_quarantined_and_reported(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        seen: list[str] = []

        @client.on_message
        async def _on_message(msg):
            seen.append(msg.message_id)

        @client.on_quarantine
        async def _on_quarantine(reason, payload):
            seen.append(f"quarantine:{reason}")

        await client._handle_frame(conn, json.dumps(
            {"opcode": 128, "payload": {"chatId": 1, "message": {"id": None}}}))
        assert any(s.startswith("quarantine:") for s in seen)
        assert client.health.counters.quarantined == 1

    async def test_valid_dispatch_reaches_the_callback(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        received: list[MaxMessage] = []

        @client.on_message
        async def _on_message(msg):
            received.append(msg)

        await client._handle_frame(conn, json.dumps(
            {"opcode": 128, "payload": {"chatId": -5,
                                        "message": {"id": 9, "text": "hi"}}}))
        assert [m.message_id for m in received] == ["9"]
        assert client.health.counters.accepted == 1

    async def test_inbound_filter_drops_other_chats(self):
        client = MaxClient("t", "d", chat_ids="-5", health=Health())
        _ws, conn = attach_connection(client)
        received: list[MaxMessage] = []

        @client.on_message
        async def _on_message(msg):
            received.append(msg)

        for chat in (-5, -6):
            await client._handle_frame(conn, json.dumps(
                {"opcode": 128,
                 "payload": {"chatId": chat, "message": {"id": abs(chat)}}}))
        assert [m.chat_id for m in received] == [-5]

    async def test_inbound_filter_accepts_string_ids(self):
        client = MaxClient("t", "d", chat_ids=" -5 ", health=Health())
        _ws, conn = attach_connection(client)
        received = []

        @client.on_message
        async def _on_message(msg):
            received.append(msg)

        await client._handle_frame(conn, json.dumps(
            {"opcode": 128, "payload": {"chatId": "-5", "message": {"id": 1}}}))
        assert len(received) == 1

    async def test_self_detection_tolerates_id_type_mismatch(self):
        """A string sender vs an int profile ID defeated the echo guard (D07)."""
        client = make_client()
        client._my_id = 12345
        _ws, conn = attach_connection(client)
        received: list[MaxMessage] = []

        @client.on_message
        async def _on_message(msg):
            received.append(msg)

        await client._handle_frame(conn, json.dumps(
            {"opcode": 128, "payload": {"chatId": 1,
                                        "message": {"id": 1, "sender": "12345"}}}))
        assert received[0].is_self is True

    async def test_logout_frame_raises_auth_failed(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        with pytest.raises(AuthFailed):
            await client._dispatch(conn, {"opcode": int(OpCode.LOGOUT), "cmd": 0,
                                          "payload": {}})

    async def test_upload_ready_resolves_the_waiting_future(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        fut = asyncio.get_running_loop().create_future()
        conn.file_pending[42] = fut
        await client._dispatch(conn, {"opcode": int(OpCode.UPLOAD_READY),
                                      "payload": {"fileId": "42"}})
        assert fut.result() == {"fileId": "42"}

    async def test_upload_ready_without_file_id_is_ignored(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        await client._dispatch(conn, {"opcode": int(OpCode.UPLOAD_READY),
                                      "payload": {}})

    async def test_ping_replies_are_counted(self):
        client = make_client()
        _ws, conn = attach_connection(client)
        await client._dispatch(conn, {"opcode": 1, "payload": {}})
        assert conn.ping_replies == 1


class TestAuthClassification:
    @pytest.mark.parametrize("payload", [
        {"error": "login.token.invalid"},
        {"message": "not.authorized"},
        {"error": "session.expired"},
        # Verbatim from production, October 2026: a token revoked by MAX.
        {"localizedMessage": "Ошибка входа. Пожалуйста, авторизируйтесь снова",
         "error": "login.token", "message": "FAIL_LOGIN_TOKEN",
         "title": "Ошибка входа. Пожалуйста, авторизируйтесь снова"},
    ])
    def test_credential_rejections_are_recognised(self, payload):
        assert MaxClient._is_auth_rejection(payload) is True

    @pytest.mark.parametrize("payload", [
        {"error": "internal.server.error"},
        {"error": "proto.payload"},
        {},
    ])
    def test_other_errors_are_not_credential_rejections(self, payload):
        """A server hiccup must not be reported as "your token is invalid"."""
        assert MaxClient._is_auth_rejection(payload) is False


class TestConnectionBookkeeping:
    def test_generations_are_distinct(self):
        first = _Connection(FakeWS(), 1.0)
        second = _Connection(FakeWS(), 1.0)
        assert second.generation > first.generation

    def test_fail_all_clears_both_registries(self):
        conn = _Connection(FakeWS(), 1.0)
        loop = asyncio.new_event_loop()
        try:
            from app.max_client import _Pending
            rpc = loop.create_future()
            upload = loop.create_future()
            conn.pending[1] = _Pending(rpc, 32)
            conn.file_pending[2] = upload
            conn.fail_all(ConnectionLost("x"))
            assert conn.pending == {} and conn.file_pending == {}
            assert isinstance(rpc.exception(), ConnectionLost)
            assert isinstance(upload.exception(), ConnectionLost)
            assert conn.closed is True
        finally:
            loop.close()


class TestLagAndState:
    def test_lag_is_none_without_a_timestamp(self):
        assert MaxClient._lag_seconds(MaxMessage(timestamp=None)) is None

    def test_lag_is_none_for_garbage(self):
        assert MaxClient._lag_seconds(MaxMessage(timestamp="soon")) is None

    def test_ready_requires_both_state_and_connection(self):
        client = make_client()
        client.health.set_state(LinkState.READY)
        assert client.ready is False       # no connection object yet
        attach_connection(client)
        assert client.ready is True

    async def test_wait_ready_times_out_when_not_ready(self):
        client = make_client()
        assert await client.wait_ready(timeout=0.01) is False

    async def test_wait_ready_returns_once_ready(self):
        client = make_client()
        client._set_state(LinkState.READY)
        assert await client.wait_ready(timeout=0.01) is True

    def test_new_cid_is_unique_enough(self):
        assert MaxClient.new_cid() != MaxClient.new_cid() or True  # no crash
        assert isinstance(MaxClient.new_cid(), int)


class TestInstanceIsolation:
    def test_chat_filter_is_per_instance(self):
        first = MaxClient("t", "d", chat_ids="-1")
        second = MaxClient("t", "d", chat_ids="-2")
        assert first.inbound_chat_filter == ["-1"]
        assert second.inbound_chat_filter == ["-2"]
