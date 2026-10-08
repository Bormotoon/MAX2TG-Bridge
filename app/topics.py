"""The one place that maps a MAX chat to a Telegram forum topic.

Everything that creates, renames or removes a topic goes through here —
incoming messages, ``/bind``, ``/add`` and ``/del`` alike. Previously the
sender held a lock the commands did not use, so a MAX message and a ``/bind``
arriving together could each decide the chat was unbound and create their own
topic (audit T04).

The other half of the fix is in :mod:`app.store`: the mapping is a transactional
row with a unique constraint in both directions, so re-binding a chat cannot
leave the old thread quietly forwarding into it (audit T03), and a corrupt state
file refuses to load rather than being replaced by an empty one (audit T01).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from dataclasses import dataclass
from typing import Any

from app.errors import describe_exception
from app.store import (
    TITLE_MANUAL,
    TITLE_PLACEHOLDER,
    TITLE_RESOLVED,
    TOPIC_ACTIVE,
    Store,
    TopicRecord,
    chat_key,
)

log = logging.getLogger(__name__)

TG_TOPIC_NAME_MAX = 128

# Marker written before a topic is created and cleared once the mapping is
# stored. What survives a crash between the two is an orphan topic.
_INTENT_PREFIX = "pending_topic:"

# Telegram's wording for "that thread is not there any more". Matched loosely
# because the API has never promised a stable phrasing.
_TOPIC_GONE_MARKERS = (
    "message thread not found",
    "topic_deleted",
    "topic deleted",
    "thread not found",
    "topic_closed",
    "topic closed",
)


def is_topic_gone_error(exc: BaseException) -> bool:
    """True when Telegram says the thread we were posting into is unusable.

    Retrying that is pointless and hiding it is worse: the mapping has to be
    reconciled or the chat has no working destination at all (audit T06).
    """
    text = str(exc).lower()
    return any(marker in text for marker in _TOPIC_GONE_MARKERS)


def classify_title(title: str) -> str:
    """Where a title came from, so a real name never loses to a placeholder.

    The old heuristic asked whether the stored string "looks numeric", which
    misread negative IDs and treated a sender's name as final — a group could
    then keep the name of whoever happened to message first (audit T07).
    """
    text = (title or "").strip()
    if not text:
        return TITLE_PLACEHOLDER
    if text.startswith("DM:"):
        return TITLE_PLACEHOLDER
    stripped = text[1:] if text[:1] == "-" else text
    if stripped.isdigit():
        return TITLE_PLACEHOLDER
    return TITLE_RESOLVED


def normalise_title(title: Any, fallback: Any) -> str:
    text = str(title or "").strip()
    if not text:
        text = str(fallback)
    return text[:TG_TOPIC_NAME_MAX]


@dataclass
class TopicResolution:
    """Outcome of asking for a chat's destination thread."""

    thread_id: int | None
    record: TopicRecord | None = None
    created: bool = False
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.thread_id is not None


class TopicService:
    def __init__(self, store: Store, bot, supergroup_id: int):
        self._store = store
        self._bot = bot
        self._supergroup_id = int(supergroup_id)
        self._locks: dict[str, asyncio.Lock] = {}

    @property
    def supergroup_id(self) -> int:
        return self._supergroup_id

    def _lock(self, chat: Any) -> asyncio.Lock:
        key = chat_key(chat)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    # ── reads ──────────────────────────────────────────────────────

    async def get_thread(self, chat: Any) -> int | None:
        record = await self._store.get_topic(chat)
        if record is None or record.state != TOPIC_ACTIVE:
            return None
        return record.thread_id

    async def get_record(self, chat: Any) -> TopicRecord | None:
        return await self._store.get_topic(chat)

    async def chat_for_thread(self, thread_id: int) -> Any | None:
        record = await self._store.topic_by_thread(int(thread_id))
        if record is None or record.state != TOPIC_ACTIVE:
            return None
        return record.chat_id

    async def all_topics(self) -> list[TopicRecord]:
        return await self._store.all_topics()

    # ── create / rename ────────────────────────────────────────────

    async def ensure_topic(self, chat: Any, title: Any,
                           origin: str | None = None) -> TopicResolution:
        """Return the thread for a chat, creating it if needed.

        Never falls back to the General topic. Posting a private chat's messages
        into General on a creation failure loses the isolation the whole design
        is built on, and makes replies un-routable (audit T06); the caller gets
        an error instead and the delivery is retried.
        """
        wanted = normalise_title(title, chat)
        wanted_origin = origin or classify_title(wanted)

        async with self._lock(chat):
            record = await self._store.get_topic(chat)
            if record is not None and record.state == TOPIC_ACTIVE:
                await self._maybe_rename(record, wanted, wanted_origin)
                return TopicResolution(record.thread_id, record)

            intent_key = f"{_INTENT_PREFIX}{chat_key(chat)}"
            await self._store.set_meta(intent_key, wanted)
            try:
                topic = await self._bot.create_forum_topic(
                    chat_id=self._supergroup_id, name=wanted)
            except Exception as exc:
                log.error(
                    "Cannot create a forum topic for MAX chat %s: %s. Is the "
                    "supergroup a forum, and is the bot an admin with "
                    "'Manage Topics'?", chat_key(chat), describe_exception(exc))
                return TopicResolution(None, error=describe_exception(exc))

            thread_id = topic.message_thread_id
            record = await self._store.set_topic(chat, thread_id, wanted,
                                                 wanted_origin)
            await self._store.clear_meta(intent_key)
            log.info("Created forum topic thread=%s for MAX chat %s",
                     thread_id, chat_key(chat))
            return TopicResolution(thread_id, record, created=True)

    async def _maybe_rename(self, record: TopicRecord, title: str,
                            origin: str) -> None:
        """Upgrade a placeholder name; never overwrite one a human chose."""
        if record.title_origin == TITLE_MANUAL:
            return
        if origin == TITLE_PLACEHOLDER:
            return
        if title == record.title:
            return
        if record.title_origin == TITLE_RESOLVED and origin != TITLE_MANUAL:
            # Both are real names — MAX renamed the chat or the peer. Follow it,
            # but only when it actually changed.
            pass
        try:
            await self._bot.edit_forum_topic(
                chat_id=self._supergroup_id,
                message_thread_id=record.thread_id,
                name=title,
            )
        except Exception as exc:
            if is_topic_gone_error(exc):
                log.warning("Topic %s is gone while renaming — dropping mapping",
                            record.thread_id)
                await self._store.remove_topic(record.chat_id,
                                               expect_thread_id=record.thread_id)
                return
            log.warning("Could not rename topic %s: %s", record.thread_id,
                        describe_exception(exc))
            return
        await self._store.update_topic_title(record.chat_id, title, origin)
        record.title = title
        record.title_origin = origin
        log.info("Renamed topic %s → %r", record.thread_id, title)

    async def bind(self, chat: Any, title: Any,
                   origin: str = TITLE_MANUAL) -> TopicResolution:
        """Explicit binding from ``/bind`` / ``/add`` — same lock, same rules."""
        return await self.ensure_topic(chat, title, origin)

    # ── removal / reconciliation ───────────────────────────────────

    async def unbind(self, chat: Any, *, expect_thread_id: int | None = None,
                     expect_revision: int | None = None) -> int | None:
        """Drop a mapping only if it is still the one the caller saw."""
        async with self._lock(chat):
            return await self._store.remove_topic(
                chat, expect_thread_id=expect_thread_id,
                expect_revision=expect_revision)

    async def delete_topic(self, chat: Any, thread_id: int,
                           revision: int | None = None) -> tuple[bool, str | None]:
        """Delete the Telegram topic, then release the mapping.

        Order matters: the mapping is released *after* Telegram confirms, so a
        failed delete does not leave a live topic that nothing routes to. If the
        thread is already gone we still release it — that is the reconciliation
        case, not a failure.
        """
        async with self._lock(chat):
            record = await self._store.get_topic(chat)
            if record is None:
                return False, "связь уже снята"
            if record.thread_id != thread_id or (
                    revision is not None and record.revision != revision):
                return False, "связь изменилась с момента запроса — ничего не удалено"

            await self._store.set_topic_state(chat, "deleting")
            try:
                await self._bot.delete_forum_topic(
                    chat_id=self._supergroup_id, message_thread_id=thread_id)
            except Exception as exc:
                if not is_topic_gone_error(exc):
                    await self._store.set_topic_state(chat, TOPIC_ACTIVE)
                    return False, describe_exception(exc)
                log.info("Topic %s was already gone — releasing the mapping",
                         thread_id)
            await self._store.remove_topic(chat, expect_thread_id=thread_id)
            return True, None

    async def reconcile_missing(self, chat: Any, thread_id: int) -> None:
        """Forget a thread Telegram says no longer exists.

        The next message for that chat creates a fresh topic instead of retrying
        forever against a dead thread.
        """
        removed = await self._store.remove_topic(chat, expect_thread_id=thread_id)
        if removed is not None:
            log.warning("Topic %s for MAX chat %s no longer exists — mapping "
                        "dropped, a new topic will be created on the next "
                        "message", thread_id, chat_key(chat))

    # ── startup checks ─────────────────────────────────────────────

    async def report_orphans(self) -> list[str]:
        """Topic creations that were started and never finished.

        Two things land here, and both deserve the operator's attention: a crash
        between ``create_forum_topic`` and the mapping write, which leaves a
        topic in the supergroup that nothing points at, and a chat whose topic
        simply could never be created. Neither can be resolved automatically —
        deleting a topic we are not sure is ours is worse than naming it.

        The scan is over the intent markers, not over the topic table: in the
        orphan case there *is* no topic row, because that write is precisely the
        one that did not happen.
        """
        orphans: list[str] = []
        markers = await self._store.meta_with_prefix(_INTENT_PREFIX)
        for key, wanted in markers.items():
            ckey = key[len(_INTENT_PREFIX):]
            record = await self._store.get_topic(ckey)
            if record is not None and record.state == TOPIC_ACTIVE:
                # The mapping did land; only the marker was left behind.
                await self._store.clear_meta(key)
                continue
            orphans.append(f"{ckey}: {wanted!r}")
        return orphans


# ── migration from the legacy JSON map ─────────────────────────────

async def migrate_json_topics(store: Store, json_path: str) -> int:
    """Import ``state/topics.json`` into the database, once.

    The old file is renamed rather than deleted: if the import misreads
    something, the original is still there to look at. Anything malformed is
    refused loudly instead of being skipped silently, because a half-imported
    topic map sends a private conversation to the wrong thread (audit T01).
    """
    def _read() -> dict | None:
        if not os.path.exists(json_path):
            return None
        with open(json_path, encoding="utf-8") as fh:
            return json.load(fh)

    if await store.get_meta("topics_json_migrated"):
        return 0

    # Off the loop: it runs once at startup, but a slow or stuck filesystem
    # should not freeze everything else that is starting up alongside it.
    data = await asyncio.to_thread(_read)
    if data is None:
        return 0
    chats = data.get("chats")
    if not isinstance(chats, dict):
        raise ValueError(f"{json_path}: 'chats' is not an object")

    imported = 0
    seen_threads: dict[int, str] = {}
    for key, rec in chats.items():
        if not isinstance(rec, dict):
            raise ValueError(f"{json_path}: entry {key!r} is not an object")
        thread_id = rec.get("topic_id")
        if thread_id is None:
            continue
        thread_id = int(thread_id)
        if thread_id in seen_threads:
            raise ValueError(
                f"{json_path}: thread {thread_id} is claimed by both "
                f"{seen_threads[thread_id]!r} and {key!r}. Resolve it by hand "
                "before starting — importing either one would route a chat "
                "to the wrong topic."
            )
        seen_threads[thread_id] = key
        title = str(rec.get("title") or key)
        try:
            chat_id: Any = int(key)
        except (TypeError, ValueError):
            chat_id = key
        await store.set_topic(chat_id, thread_id, title, classify_title(title))
        imported += 1

    await store.set_meta("topics_json_migrated", "1")
    backup = json_path + ".imported"
    try:
        await asyncio.to_thread(shutil.move, json_path, backup)
        log.info("Imported %d topic mapping(s); the old file is kept at %s",
                 imported, backup)
    except OSError:
        log.warning("Imported %d topic mapping(s) but could not rename %s",
                    imported, json_path)
    return imported
