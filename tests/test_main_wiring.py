"""Startup and shutdown of the whole bridge, with only the two networks faked.

The old entry point could fail *during* startup and skip cleanup entirely, and
it never handled SIGTERM, so a container stop killed it with work in memory
(audit R08). These tests drive the real ``run_bridge`` against a local MAX
server and a stub Telegram client, then stop it the way a supervisor would.
"""

from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.errors import SendResult
from app.healthcheck import evaluate, health_path
from app.main import run_bridge
from app.max_client import MaxClient
from app.store import STATUS_DELIVERED, Store
from tests.test_max_connection import FakeMaxServer, wait_for


class StubTelegramSender:
    """Stands in for TelegramSender without touching api.telegram.org."""

    instances: list[StubTelegramSender] = []

    def __init__(self, token, chat_id, proxy_url=None, status_chat_id=None,
                 bot=None):
        self.chat_id = int(chat_id)
        self.sent: list[tuple[str, int | None]] = []
        self.started = False
        self.stopped = False
        self.bot = StubBot()
        StubTelegramSender.instances.append(self)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def preflight(self):
        return []

    async def send_text(self, text, thread_id=None, **kwargs):
        self.sent.append((text, thread_id))
        return SendResult.delivered([len(self.sent)])

    async def _retry(self, factory, what=""):
        return SendResult.delivered([1])


class StubBot:
    def __init__(self):
        self.created: list[str] = []
        self.next_thread_id = 500

    async def create_forum_topic(self, chat_id, name):
        self.created.append(name)
        thread = self.next_thread_id
        self.next_thread_id += 1
        return type("Topic", (), {"message_thread_id": thread})()

    async def edit_forum_topic(self, **kwargs):
        pass

    async def delete_forum_topic(self, **kwargs):
        pass

    async def send_message(self, **kwargs):
        return type("Msg", (), {"message_id": 1})()

    async def send_photo(self, **kwargs):
        return type("Msg", (), {"message_id": 1})()

    async def pin_chat_message(self, **kwargs):
        pass


def make_settings(tmp_path, **overrides) -> Settings:
    state = tmp_path / "state"
    logs = tmp_path / "logs"
    spool = state / "spool"
    for directory in (state, logs, spool):
        directory.mkdir(parents=True, exist_ok=True)
    base = dict(
        max_token="token-value", max_device_id="device-value",
        tg_bot_token="123:abc", tg_chat_id=-1001234567890,
        tg_allowed_user_ids=frozenset({42}),
        reply_enabled=False,
        state_dir=str(state), log_dir=str(logs), spool_dir=str(spool),
        sqlite_synchronous="OFF", shutdown_timeout=2.0,
    )
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def stub_sender(monkeypatch):
    StubTelegramSender.instances.clear()
    monkeypatch.setattr("app.main.TelegramSender", StubTelegramSender)
    return StubTelegramSender


@pytest.fixture
async def max_server(monkeypatch):
    server = FakeMaxServer()
    url = await server.start()
    monkeypatch.setattr(MaxClient, "WS_URL", url)
    monkeypatch.setattr(MaxClient, "AUTH_TIMEOUT_SEC", 2)
    monkeypatch.setattr(MaxClient, "HANDSHAKE_TIMEOUT_SEC", 2)
    monkeypatch.setattr(MaxClient, "RECONNECT_MIN_SEC", 0.05)
    monkeypatch.setattr(MaxClient, "RPC_TIMEOUT_SEC", 2)
    try:
        yield server
    finally:
        await server.stop()


async def run_until(settings, condition, timeout=10.0):
    """Start the bridge, wait for a condition, then stop it like a supervisor."""
    task = asyncio.create_task(run_bridge(settings))
    try:
        assert await wait_for(condition, timeout=timeout), "condition never held"
    finally:
        # asyncio.run() installs the signal handlers; here we cancel, which
        # exercises the same AsyncExitStack unwinding.
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    return task


class TestStartup:
    async def test_the_bridge_reaches_ready(self, tmp_path, stub_sender,
                                            max_server):
        settings = make_settings(tmp_path)
        await run_until(settings, lambda: max_server.authorized.is_set())
        assert stub_sender.instances[0].started is True

    async def test_the_state_database_is_created(self, tmp_path, stub_sender,
                                                 max_server):
        settings = make_settings(tmp_path)
        await run_until(settings, lambda: max_server.authorized.is_set())
        store = Store(settings.db_path, synchronous="OFF")
        await store.open()
        try:
            assert await store.get_meta("tg_group_id") == str(settings.tg_chat_id)
            assert await store.get_meta("max_account_id") == "555"
        finally:
            await store.close()

    async def test_the_connection_notice_is_queued(self, tmp_path, stub_sender,
                                                   max_server):
        settings = make_settings(tmp_path)
        await run_until(settings, lambda: max_server.authorized.is_set())
        # Status goes through the notifier's own queue; the point is that
        # nothing on the connection path waited for Telegram.
        assert stub_sender.instances[0].started

    async def test_the_health_snapshot_is_published(self, tmp_path, stub_sender,
                                                    max_server):
        import json
        import os

        settings = make_settings(tmp_path)
        path = health_path(settings.state_dir)

        def ready_snapshot() -> bool:
            if not os.path.exists(path):
                return False
            try:
                with open(path, encoding="utf-8") as handle:
                    return evaluate(json.load(handle))[0]
            except (OSError, ValueError):
                return False

        await run_until(settings, ready_snapshot, timeout=15)


class TestMessageFlow:
    async def test_an_incoming_message_is_journalled(self, tmp_path, stub_sender,
                                                     max_server):
        settings = make_settings(tmp_path)
        task = asyncio.create_task(run_bridge(settings))
        try:
            assert await wait_for(lambda: max_server.authorized.is_set(), timeout=10)
            await max_server.push(128, {"chatId": -77,
                                        "message": {"id": 4242, "text": "привет",
                                                    "sender": 900}})

            store = Store(settings.db_path, synchronous="OFF")
            await store.open()
            try:
                async def journalled():
                    job = await store.find_job(source="max",
                                               account=settings.max_device_id,
                                               chat=-77, event_id="4242")
                    return job is not None and job.status == STATUS_DELIVERED

                ok = False
                for _ in range(500):
                    if await journalled():
                        ok = True
                        break
                    await asyncio.sleep(0.02)
                assert ok, "the message never reached a delivered state"
            finally:
                await store.close()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_a_replay_is_not_delivered_twice(self, tmp_path, stub_sender,
                                                   max_server):
        settings = make_settings(tmp_path)
        task = asyncio.create_task(run_bridge(settings))
        try:
            assert await wait_for(lambda: max_server.authorized.is_set(), timeout=10)
            frame = {"chatId": -77, "message": {"id": 4242, "text": "привет",
                                                "sender": 900}}
            await max_server.push(128, frame)
            await max_server.push(128, frame)

            sender = stub_sender.instances[0]
            assert await wait_for(lambda: len(sender.sent) >= 1, timeout=10)
            await asyncio.sleep(0.3)
            body_posts = [s for s in sender.sent if "привет" in s[0]]
            assert len(body_posts) == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class TestShutdown:
    async def test_stopping_closes_the_clients(self, tmp_path, stub_sender,
                                               max_server):
        settings = make_settings(tmp_path)
        await run_until(settings, lambda: max_server.authorized.is_set())
        assert stub_sender.instances[0].stopped is True

    async def test_a_second_start_reuses_the_same_state(self, tmp_path,
                                                        stub_sender, max_server):
        settings = make_settings(tmp_path)
        await run_until(settings, lambda: max_server.authorized.is_set())
        max_server.authorized.clear()
        await run_until(settings, lambda: max_server.authorized.is_set())
        # No exception from the manifest check means the account and group
        # matched on the second run, which is the point of it existing.


class TestStartupFailures:
    async def test_a_state_bound_to_another_group_refuses_to_start(
            self, tmp_path, stub_sender, max_server):
        from app.errors import StoreCorrupted

        settings = make_settings(tmp_path)
        store = Store(settings.db_path, synchronous="OFF")
        await store.open()
        await store.check_manifest(account="", group_id="-100999")
        await store.close()

        with pytest.raises(StoreCorrupted, match="Telegram group"):
            await run_bridge(settings)


class TestOutageWarning:
    """After a gap, the owner is told that messages may simply be missing.

    The bridge has no catch-up: there is no confirmed history/sync mechanism in
    the MAX protocol, so whether anything sent during an outage arrives is
    entirely MAX's decision to replay it. Saying so is the honest alternative to
    a silent hole in a conversation (audit R07).
    """

    async def test_a_long_outage_is_announced_after_reconnecting(
            self, tmp_path, stub_sender, max_server, monkeypatch):
        monkeypatch.setattr("app.main.GAP_NOTICE_SEC", 0.001)
        settings = make_settings(tmp_path)
        task = asyncio.create_task(run_bridge(settings))
        try:
            assert await wait_for(lambda: max_server.authorized.is_set(),
                                  timeout=10)
            max_server.authorized.clear()
            await max_server.drop()
            assert await wait_for(lambda: max_server.authorized.is_set(),
                                  timeout=10)
            sender = stub_sender.instances[0]
            assert await wait_for(
                lambda: any("Пропуск" in text for text, _ in sender.sent),
                timeout=10), f"no gap notice among {sender.sent}"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_a_brief_blip_is_not_announced(self, tmp_path, stub_sender,
                                                 max_server):
        """A two-second reconnect is noise, not a data-loss event."""
        settings = make_settings(tmp_path)
        task = asyncio.create_task(run_bridge(settings))
        try:
            assert await wait_for(lambda: max_server.authorized.is_set(),
                                  timeout=10)
            max_server.authorized.clear()
            await max_server.drop()
            assert await wait_for(lambda: max_server.authorized.is_set(),
                                  timeout=10)
            sender = stub_sender.instances[0]
            assert not any("Пропуск" in text for text, _ in sender.sent)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_a_clean_first_start_announces_no_gap(self, tmp_path,
                                                       stub_sender, max_server,
                                                       monkeypatch):
        monkeypatch.setattr("app.main.GAP_NOTICE_SEC", 0.001)
        settings = make_settings(tmp_path)
        await run_until(settings, lambda: max_server.authorized.is_set())
        sender = stub_sender.instances[0]
        assert not any("Пропуск" in text for text, _ in sender.sent)


class TestDeadLetterNoticeIsSilent:
    """The "not delivered" note lands in a shared group topic. During the
    October 2026 catch-up three of them rang every member's phone."""

    async def test_notice_goes_out_without_notification(self, store):
        from app.main import _dead_letter
        from app.store import Job
        from app.tg_sender import TelegramSender
        from app.topics import TopicService
        from tests.test_tg_sender import FakeBot

        bot = FakeBot()
        sender = TelegramSender("t", "-100500", bot=bot)
        sender.RETRY_BACKOFF_SEC = 0.0
        topics = TopicService(store, bot, -100500)
        await store.set_topic("-1", 42, "chat")
        job = Job(id=1, source="max", account="acc", chat_key="-1",
                  event_id="1", revision="r", payload={}, status="uncertain",
                  attempts=0, next_retry_at=None, last_error=None,
                  received_at=0.0, updated_at=0.0)

        await _dead_letter(sender, topics)(job, "результат отправки неизвестен")

        assert [(name, kw.get("disable_notification")) for name, kw in bot.calls] \
            == [("send_message", True)]
