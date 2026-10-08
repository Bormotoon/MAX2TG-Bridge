"""Shared fixtures. Deterministic by construction — no sleeps to race against."""

from __future__ import annotations

import asyncio

import pytest

from app.health import Health
from app.net import MediaSpool
from app.store import Store


@pytest.fixture
async def store(tmp_path):
    db = Store(str(tmp_path / "bridge.sqlite3"), synchronous="OFF")
    await db.open()
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture
def health():
    return Health()


@pytest.fixture
def spool(tmp_path):
    return MediaSpool(str(tmp_path / "spool"), max_total_bytes=1024 * 1024)


class FakeBot:
    """The slice of the Bot API the topic service and sender actually use."""

    def __init__(self):
        self.created: list[str] = []
        self.deleted: list[int] = []
        self.renamed: list[tuple[int, str]] = []
        self.next_thread_id = 100
        self.create_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.rename_error: Exception | None = None

    async def create_forum_topic(self, chat_id, name):
        if self.create_error:
            raise self.create_error
        self.created.append(name)
        thread_id = self.next_thread_id
        self.next_thread_id += 1
        return type("Topic", (), {"message_thread_id": thread_id})()

    async def edit_forum_topic(self, chat_id, message_thread_id, name):
        if self.rename_error:
            raise self.rename_error
        self.renamed.append((message_thread_id, name))

    async def delete_forum_topic(self, chat_id, message_thread_id):
        if self.delete_error:
            raise self.delete_error
        self.deleted.append(message_thread_id)


@pytest.fixture
def fake_bot():
    return FakeBot()


async def drain(condition, timeout: float = 2.0, interval: float = 0.01):
    """Wait for a condition instead of sleeping a guessed amount of time."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.fixture(autouse=True)
def _no_group_pacing(monkeypatch):
    """Pacing guards production against flood control; in tests it only
    turns every multi-message scenario into a real wait. ``_GroupPacer`` has
    its own tests."""
    from app.tg_sender import TelegramSender
    monkeypatch.setattr(TelegramSender, "GROUP_MSGS_PER_MIN", 0)
