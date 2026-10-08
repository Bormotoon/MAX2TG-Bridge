"""SQLite-backed durable state: the delivery journal, topic map and cursors.

Why a database at all, for a single-user bridge: the old design remembered a
message ID in an in-memory ``OrderedDict`` *before* trying to deliver it, so any
failure between "accepted" and "posted to Telegram" turned into permanent,
silent loss — the retry from MAX was recognised as a duplicate and dropped. A
restart lost the entire history. The journal here is the thing that makes
"received" and "delivered" two different facts (audit D01/D04).

All SQL runs on one dedicated thread. Every statement is short and local, but
running them inline would still put file I/O and fsync on the event loop, which
is exactly the stall the audit flagged elsewhere (R09).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, TypeVar

from app.errors import StoreCorrupted

log = logging.getLogger(__name__)

T = TypeVar("T")

SCHEMA_VERSION = 2

# Job lifecycle. `processing` exists so a crash mid-delivery is visible on the
# next start: those jobs go to recovery rather than being replayed blindly.
STATUS_RECEIVED = "received"
STATUS_PROCESSING = "processing"
STATUS_DELIVERED = "delivered"
STATUS_RETRYABLE = "retryable_failed"
STATUS_PERMANENT = "permanent_failed"
STATUS_UNCERTAIN = "uncertain"
STATUS_QUARANTINED = "quarantined"

ACTIVE_STATUSES = (STATUS_RECEIVED, STATUS_PROCESSING, STATUS_RETRYABLE)

PART_PENDING = "pending"
PART_CONFIRMED = "confirmed"
PART_UNCERTAIN = "uncertain"
PART_FAILED = "failed"
PART_SKIPPED = "skipped"

TITLE_MANUAL = "manual"
TITLE_PLACEHOLDER = "placeholder"
TITLE_RESOLVED = "resolved"

TOPIC_ACTIVE = "active"
TOPIC_DELETING = "deleting"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT    NOT NULL,
    account       TEXT    NOT NULL,
    chat_key      TEXT    NOT NULL,
    event_id      TEXT    NOT NULL,
    revision      TEXT    NOT NULL,
    payload       TEXT    NOT NULL,
    status        TEXT    NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,
    last_error    TEXT,
    received_at   REAL    NOT NULL,
    updated_at    REAL    NOT NULL,
    UNIQUE (source, account, chat_key, event_id, revision)
);

CREATE INDEX IF NOT EXISTS inbox_chat_order
    ON inbox (source, account, chat_key, id);
CREATE INDEX IF NOT EXISTS inbox_status
    ON inbox (status, next_retry_at);

CREATE TABLE IF NOT EXISTS delivery_parts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES inbox(id) ON DELETE CASCADE,
    part_key        TEXT    NOT NULL,
    kind            TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    destination_ids TEXT,
    last_error      TEXT,
    updated_at      REAL    NOT NULL,
    UNIQUE (job_id, part_key)
);

CREATE TABLE IF NOT EXISTS topics (
    chat_key     TEXT    PRIMARY KEY,
    chat_id_json TEXT    NOT NULL,
    thread_id    INTEGER NOT NULL UNIQUE,
    title        TEXT    NOT NULL,
    title_origin TEXT    NOT NULL,
    state        TEXT    NOT NULL DEFAULT 'active',
    revision     INTEGER NOT NULL DEFAULT 1,
    created_at   REAL    NOT NULL,
    updated_at   REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS message_map (
    account    TEXT    NOT NULL,
    chat_key   TEXT    NOT NULL,
    max_id     TEXT    NOT NULL,
    tg_id      INTEGER NOT NULL,
    created_at REAL    NOT NULL,
    PRIMARY KEY (account, chat_key, max_id, tg_id)
);

CREATE INDEX IF NOT EXISTS message_map_by_tg
    ON message_map (account, chat_key, tg_id);

CREATE TABLE IF NOT EXISTS cursors (
    name       TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""


def chat_key(chat_id: Any) -> str:
    """Normalise a chat ID so ``-42`` and ``"-42"`` are one chat.

    Mixed int/str IDs flowed through the old code untouched, which meant the
    same chat could hold two topic rows and two dedup namespaces (audit D02/T07).
    """
    if chat_id is None:
        return ""
    if isinstance(chat_id, bool):
        return str(chat_id)
    if isinstance(chat_id, int):
        return str(chat_id)
    text = str(chat_id).strip()
    try:
        return str(int(text))
    except (TypeError, ValueError):
        return text


def revision_hash(payload: Any) -> str:
    """Stable short hash of a normalised event body.

    Two DISPATCH frames for one message ID are only "the same event" when their
    meaningful content matches. A link preview the server attaches after the
    fact changes the content, so it gets its own revision and can be handled as
    an edit instead of being mistaken for a replay (audit D05).
    """
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


@dataclass
class Job:
    """One durable unit of work: an accepted event that still owes a delivery."""

    id: int
    source: str
    account: str
    chat_key: str
    event_id: str
    revision: str
    payload: dict
    status: str
    attempts: int
    next_retry_at: float | None
    last_error: str | None
    received_at: float
    updated_at: float

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Job:
        return cls(
            id=row["id"],
            source=row["source"],
            account=row["account"],
            chat_key=row["chat_key"],
            event_id=row["event_id"],
            revision=row["revision"],
            payload=json.loads(row["payload"]),
            status=row["status"],
            attempts=row["attempts"],
            next_retry_at=row["next_retry_at"],
            last_error=row["last_error"],
            received_at=row["received_at"],
            updated_at=row["updated_at"],
        )


@dataclass
class Part:
    """One confirmed-or-not piece of a job's delivery (a text block, an album…)."""

    part_key: str
    kind: str
    status: str
    destination_ids: list[int]
    last_error: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Part:
        raw = row["destination_ids"]
        return cls(
            part_key=row["part_key"],
            kind=row["kind"],
            status=row["status"],
            destination_ids=json.loads(raw) if raw else [],
            last_error=row["last_error"],
        )


@dataclass
class TopicRecord:
    chat_key: str
    chat_id: Any
    thread_id: int
    title: str
    title_origin: str
    state: str
    revision: int


class Store:
    """Async facade over a single SQLite connection pinned to one thread."""

    def __init__(self, path: str, *, synchronous: str = "FULL",
                 retention_days: float = 14.0):
        self._path = path
        self._synchronous = synchronous.upper()
        self._retention = max(0.0, retention_days) * 86400.0
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="store")
        self._conn: sqlite3.Connection | None = None

    # ── lifecycle ──────────────────────────────────────────────────

    async def open(self) -> None:
        await self._run(self._open_sync)

    async def close(self) -> None:
        try:
            await self._run(self._close_sync)
        finally:
            self._executor.shutdown(wait=True)

    def _open_sync(self) -> None:
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)
        if not os.access(directory, os.W_OK):
            raise StoreCorrupted(f"state directory is not writable: {directory}")
        conn = sqlite3.connect(self._path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"PRAGMA synchronous={self._synchronous}")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.executescript(_SCHEMA)
        except sqlite3.DatabaseError as exc:
            conn.close()
            # Refusing to start beats starting with an empty map: the previous
            # design logged the failure and carried on with no topics at all,
            # then overwrote the original file on the next binding (audit T01).
            raise StoreCorrupted(
                f"{self._path} is not a usable state database: {exc}. "
                "Move it aside (do not delete it) and restart to rebuild."
            ) from exc
        self._conn = conn
        self._set_meta_sync("schema_version", str(SCHEMA_VERSION))

    def _close_sync(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def _run(self, fn: Callable[..., T], *args) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, *args)

    @property
    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StoreCorrupted("store is not open")
        return self._conn

    # ── manifest ───────────────────────────────────────────────────

    async def check_manifest(self, *, account: str, group_id: str) -> None:
        """Bind this state file to one MAX account and one Telegram group.

        Pointing the bridge at a different supergroup while keeping the old
        state silently reused thread IDs that belong to another chat (audit
        O05). Now it refuses to start instead.
        """
        await self._run(self._check_manifest_sync, account, group_id)

    def _check_manifest_sync(self, account: str, group_id: str) -> None:
        stored_account = self._get_meta_sync("max_account_id")
        stored_group = self._get_meta_sync("tg_group_id")
        if stored_account and account and stored_account != account:
            raise StoreCorrupted(
                f"state at {self._path} belongs to MAX account {stored_account}, "
                f"but the bridge is starting as {account}. Use a separate "
                "STATE_DIR per account."
            )
        if stored_group and group_id and stored_group != group_id:
            raise StoreCorrupted(
                f"state at {self._path} belongs to Telegram group {stored_group}, "
                f"but TG_CHAT_ID is {group_id}. Topic IDs are per-group and "
                "would point at the wrong threads. Use a separate STATE_DIR."
            )
        if account:
            self._set_meta_sync("max_account_id", account)
        if group_id:
            self._set_meta_sync("tg_group_id", group_id)

    async def get_meta(self, key: str) -> str | None:
        return await self._run(self._get_meta_sync, key)

    async def set_meta(self, key: str, value: str) -> None:
        await self._run(self._set_meta_sync, key, value)

    def _get_meta_sync(self, key: str) -> str | None:
        row = self._db.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    async def meta_with_prefix(self, prefix: str) -> dict[str, str]:
        """Every non-empty meta value whose key starts with ``prefix``.

        Used to find the markers left by operations that never finished — a
        topic creation interrupted between Telegram accepting it and the
        mapping being written leaves nothing else behind to look for.
        """
        return await self._run(self._meta_with_prefix_sync, prefix)

    def _meta_with_prefix_sync(self, prefix: str) -> dict[str, str]:
        rows = self._db.execute(
            "SELECT key, value FROM meta WHERE key LIKE ? ESCAPE '\\' "
            "AND value != '' ORDER BY key",
            (prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
             + "%",),
        ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    async def clear_meta(self, key: str) -> None:
        await self._run(self._clear_meta_sync, key)

    def _clear_meta_sync(self, key: str) -> None:
        self._db.execute("DELETE FROM meta WHERE key = ?", (key,))

    def _set_meta_sync(self, key: str, value: str) -> None:
        self._db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    # ── inbox ──────────────────────────────────────────────────────

    async def enqueue(self, *, source: str, account: str, chat: Any,
                      event_id: str, revision: str, payload: dict) -> Job | None:
        """Record an accepted event. Returns ``None`` if it is a known replay.

        The UNIQUE constraint is the dedup — and unlike the old bounded cache it
        cannot forget an entry that has not been delivered yet, because
        completed rows are only pruned after the retention window.
        """
        return await self._run(self._enqueue_sync, source, account,
                               chat_key(chat), event_id, revision, payload)

    def _enqueue_sync(self, source: str, account: str, ckey: str,
                      event_id: str, revision: str, payload: dict) -> Job | None:
        now = time.time()
        db = self._db
        try:
            db.execute("BEGIN IMMEDIATE")
            cur = db.execute(
                "INSERT INTO inbox (source, account, chat_key, event_id, revision,"
                " payload, status, attempts, next_retry_at, last_error,"
                " received_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?)",
                (source, account, ckey, event_id, revision,
                 json.dumps(payload, ensure_ascii=False, default=str),
                 STATUS_RECEIVED, now, now),
            )
            row = db.execute(
                "SELECT * FROM inbox WHERE id = ?", (cur.lastrowid,)).fetchone()
            db.execute("COMMIT")
            return Job.from_row(row)
        except sqlite3.IntegrityError:
            db.execute("ROLLBACK")
            return None
        except Exception:
            db.execute("ROLLBACK")
            raise

    async def next_job(self, *, source: str, account: str, chat: Any,
                       now: float | None = None) -> Job | None:
        """Oldest actionable job for a chat, honouring retry backoff."""
        return await self._run(self._next_job_sync, source, account,
                               chat_key(chat), now if now is not None else time.time())

    def _next_job_sync(self, source: str, account: str, ckey: str,
                       now: float) -> Job | None:
        row = self._db.execute(
            "SELECT * FROM inbox WHERE source = ? AND account = ? AND chat_key = ?"
            "   AND status IN (?, ?, ?)"
            "   AND (next_retry_at IS NULL OR next_retry_at <= ?)"
            " ORDER BY id LIMIT 1",
            (source, account, ckey, STATUS_RECEIVED, STATUS_PROCESSING,
             STATUS_RETRYABLE, now),
        ).fetchone()
        return Job.from_row(row) if row else None

    async def peek_jobs(self, *, source: str, account: str, chat: Any,
                        after_id: int, limit: int) -> list[Job]:
        """Look ahead in one chat's queue without claiming anything.

        The album scheduler uses this: grouping is decided *before* any download
        starts, and the same worker then sends the group, so a slow album can no
        longer be overtaken by the text that followed it (audit M01/M04).
        """
        return await self._run(self._peek_jobs_sync, source, account,
                               chat_key(chat), after_id, limit)

    def _peek_jobs_sync(self, source: str, account: str, ckey: str,
                        after_id: int, limit: int) -> list[Job]:
        rows = self._db.execute(
            "SELECT * FROM inbox WHERE source = ? AND account = ? AND chat_key = ?"
            "   AND id > ? AND status IN (?, ?, ?)"
            " ORDER BY id LIMIT ?",
            (source, account, ckey, after_id, STATUS_RECEIVED,
             STATUS_PROCESSING, STATUS_RETRYABLE, limit),
        ).fetchall()
        return [Job.from_row(r) for r in rows]

    async def active_chats(self, *, source: str, account: str) -> list[str]:
        """Chat keys with unfinished work — the recovery list at startup."""
        return await self._run(self._active_chats_sync, source, account)

    def _active_chats_sync(self, source: str, account: str) -> list[str]:
        rows = self._db.execute(
            "SELECT DISTINCT chat_key FROM inbox"
            " WHERE source = ? AND account = ? AND status IN (?, ?, ?)"
            " ORDER BY chat_key",
            (source, account, STATUS_RECEIVED, STATUS_PROCESSING, STATUS_RETRYABLE),
        ).fetchall()
        return [r["chat_key"] for r in rows]

    async def mark_processing(self, job_id: int) -> None:
        await self._run(self._mark_processing_sync, job_id)

    def _mark_processing_sync(self, job_id: int) -> None:
        self._db.execute(
            "UPDATE inbox SET status = ?, attempts = attempts + 1, updated_at = ?"
            " WHERE id = ?",
            (STATUS_PROCESSING, time.time(), job_id),
        )

    async def finish_job(self, job_id: int, status: str, *,
                         error: str | None = None,
                         next_retry_at: float | None = None) -> None:
        await self._run(self._finish_job_sync, job_id, status, error, next_retry_at)

    def _finish_job_sync(self, job_id: int, status: str, error: str | None,
                         next_retry_at: float | None) -> None:
        self._db.execute(
            "UPDATE inbox SET status = ?, last_error = ?, next_retry_at = ?,"
            " updated_at = ? WHERE id = ?",
            (status, error, next_retry_at, time.time(), job_id),
        )

    async def recover_processing(self, *, source: str, account: str) -> int:
        """Return jobs interrupted by a crash to a retryable state.

        They keep their confirmed parts, so recovery resumes the missing pieces
        instead of re-posting the whole message.
        """
        return await self._run(self._recover_processing_sync, source, account)

    def _recover_processing_sync(self, source: str, account: str) -> int:
        cur = self._db.execute(
            "UPDATE inbox SET status = ?, next_retry_at = NULL, updated_at = ?,"
            " last_error = COALESCE(last_error, 'interrupted by restart')"
            " WHERE source = ? AND account = ? AND status = ?",
            (STATUS_RETRYABLE, time.time(), source, account, STATUS_PROCESSING),
        )
        return cur.rowcount or 0

    async def get_job(self, job_id: int) -> Job | None:
        return await self._run(self._get_job_sync, job_id)

    def _get_job_sync(self, job_id: int) -> Job | None:
        row = self._db.execute(
            "SELECT * FROM inbox WHERE id = ?", (job_id,)).fetchone()
        return Job.from_row(row) if row else None

    async def find_job(self, *, source: str, account: str, chat: Any,
                       event_id: str) -> Job | None:
        """Latest revision recorded for one source message ID."""
        return await self._run(self._find_job_sync, source, account,
                               chat_key(chat), event_id)

    def _find_job_sync(self, source: str, account: str, ckey: str,
                       event_id: str) -> Job | None:
        row = self._db.execute(
            "SELECT * FROM inbox WHERE source = ? AND account = ? AND chat_key = ?"
            "   AND event_id = ? ORDER BY id DESC LIMIT 1",
            (source, account, ckey, event_id),
        ).fetchone()
        return Job.from_row(row) if row else None

    async def job_history(self, *, source: str, account: str, chat: Any,
                          event_id: str, before_id: int) -> list[Job]:
        """Earlier journal rows for one source message — its previous revisions.

        An edit arrives as a new revision of a known message ID; to reflect it
        in Telegram rather than post it again we need the message IDs the
        previous revision produced (audit D05).
        """
        return await self._run(self._job_history_sync, source, account,
                               chat_key(chat), event_id, before_id)

    def _job_history_sync(self, source: str, account: str, ckey: str,
                          event_id: str, before_id: int) -> list[Job]:
        rows = self._db.execute(
            "SELECT * FROM inbox WHERE source = ? AND account = ? AND chat_key = ?"
            "   AND event_id = ? AND id < ? ORDER BY id DESC LIMIT 5",
            (source, account, ckey, event_id, before_id),
        ).fetchall()
        return [Job.from_row(r) for r in rows]

    async def requeue_failed(self, *, source: str, account: str,
                             chat: Any | None = None, limit: int = 100) -> int:
        """Hand failed / uncertain jobs back to the scheduler (the ``/retry`` command)."""
        return await self._run(self._requeue_failed_sync, source, account,
                               chat_key(chat) if chat is not None else None, limit)

    def _requeue_failed_sync(self, source: str, account: str,
                             ckey: str | None, limit: int) -> int:
        params: list[Any] = [source, account, STATUS_PERMANENT, STATUS_UNCERTAIN]
        sql = ("SELECT id FROM inbox WHERE source = ? AND account = ?"
               " AND status IN (?, ?)")
        if ckey is not None:
            sql += " AND chat_key = ?"
            params.append(ckey)
        sql += " ORDER BY id LIMIT ?"
        params.append(limit)
        ids = [r["id"] for r in self._db.execute(sql, params).fetchall()]
        if not ids:
            return 0
        marks = ",".join("?" * len(ids))
        self._db.execute(
            f"UPDATE inbox SET status = ?, attempts = 0, next_retry_at = NULL,"
            f" updated_at = ? WHERE id IN ({marks})",
            [STATUS_RETRYABLE, time.time(), *ids],
        )
        return len(ids)

    # ── delivery parts ─────────────────────────────────────────────

    async def parts(self, job_id: int) -> dict[str, Part]:
        return await self._run(self._parts_sync, job_id)

    def _parts_sync(self, job_id: int) -> dict[str, Part]:
        rows = self._db.execute(
            "SELECT * FROM delivery_parts WHERE job_id = ? ORDER BY id",
            (job_id,)).fetchall()
        return {r["part_key"]: Part.from_row(r) for r in rows}

    async def record_part(self, job_id: int, part_key: str, kind: str,
                          status: str, *, destination_ids: Iterable[int] = (),
                          error: str | None = None) -> None:
        await self._run(self._record_part_sync, job_id, part_key, kind, status,
                        list(destination_ids), error)

    def _record_part_sync(self, job_id: int, part_key: str, kind: str,
                          status: str, destination_ids: list[int],
                          error: str | None) -> None:
        self._db.execute(
            "INSERT INTO delivery_parts (job_id, part_key, kind, status,"
            " destination_ids, last_error, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(job_id, part_key) DO UPDATE SET"
            "   kind = excluded.kind, status = excluded.status,"
            "   destination_ids = excluded.destination_ids,"
            "   last_error = excluded.last_error,"
            "   updated_at = excluded.updated_at",
            (job_id, part_key, kind, status,
             json.dumps(destination_ids) if destination_ids else None,
             error, time.time()),
        )

    # ── topics ─────────────────────────────────────────────────────

    async def get_topic(self, chat: Any) -> TopicRecord | None:
        return await self._run(self._get_topic_sync, chat_key(chat))

    def _get_topic_sync(self, ckey: str) -> TopicRecord | None:
        row = self._db.execute(
            "SELECT * FROM topics WHERE chat_key = ?", (ckey,)).fetchone()
        return self._topic_from_row(row) if row else None

    async def topic_by_thread(self, thread_id: int) -> TopicRecord | None:
        return await self._run(self._topic_by_thread_sync, int(thread_id))

    def _topic_by_thread_sync(self, thread_id: int) -> TopicRecord | None:
        row = self._db.execute(
            "SELECT * FROM topics WHERE thread_id = ?", (thread_id,)).fetchone()
        return self._topic_from_row(row) if row else None

    async def all_topics(self) -> list[TopicRecord]:
        return await self._run(self._all_topics_sync)

    def _all_topics_sync(self) -> list[TopicRecord]:
        rows = self._db.execute("SELECT * FROM topics ORDER BY chat_key").fetchall()
        return [self._topic_from_row(r) for r in rows]

    @staticmethod
    def _topic_from_row(row: sqlite3.Row) -> TopicRecord:
        return TopicRecord(
            chat_key=row["chat_key"],
            chat_id=json.loads(row["chat_id_json"]),
            thread_id=row["thread_id"],
            title=row["title"],
            title_origin=row["title_origin"],
            state=row["state"],
            revision=row["revision"],
        )

    async def set_topic(self, chat: Any, thread_id: int, title: str,
                        title_origin: str = TITLE_RESOLVED) -> TopicRecord:
        """Bind a chat to a thread, enforcing uniqueness in both directions.

        The JSON store kept a stale reverse entry when a chat was re-bound, so
        two threads went on forwarding into one MAX chat (audit T03). Here the
        remap happens in one transaction and the old reverse row is gone with it.
        """
        return await self._run(self._set_topic_sync, chat_key(chat), chat,
                               int(thread_id), title, title_origin)

    def _set_topic_sync(self, ckey: str, chat_id: Any, thread_id: int,
                        title: str, title_origin: str) -> TopicRecord:
        now = time.time()
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        try:
            # Whoever else held this thread loses it: a thread ID is unique in
            # the supergroup, so two mappings to it cannot both be right.
            db.execute("DELETE FROM topics WHERE thread_id = ? AND chat_key != ?",
                       (thread_id, ckey))
            existing = db.execute(
                "SELECT revision FROM topics WHERE chat_key = ?", (ckey,)).fetchone()
            revision = (existing["revision"] + 1) if existing else 1
            db.execute(
                "INSERT INTO topics (chat_key, chat_id_json, thread_id, title,"
                " title_origin, state, revision, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(chat_key) DO UPDATE SET"
                "   chat_id_json = excluded.chat_id_json,"
                "   thread_id = excluded.thread_id,"
                "   title = excluded.title,"
                "   title_origin = excluded.title_origin,"
                "   state = excluded.state,"
                "   revision = excluded.revision,"
                "   updated_at = excluded.updated_at",
                (ckey, json.dumps(chat_id, default=str), thread_id, title,
                 title_origin, TOPIC_ACTIVE, revision, now, now),
            )
            row = db.execute("SELECT * FROM topics WHERE chat_key = ?",
                             (ckey,)).fetchone()
            db.execute("COMMIT")
            return self._topic_from_row(row)
        except Exception:
            db.execute("ROLLBACK")
            raise

    async def update_topic_title(self, chat: Any, title: str,
                                 title_origin: str) -> None:
        await self._run(self._update_topic_title_sync, chat_key(chat), title,
                        title_origin)

    def _update_topic_title_sync(self, ckey: str, title: str,
                                 title_origin: str) -> None:
        self._db.execute(
            "UPDATE topics SET title = ?, title_origin = ?, updated_at = ?"
            " WHERE chat_key = ?",
            (title, title_origin, time.time(), ckey),
        )

    async def set_topic_state(self, chat: Any, state: str) -> None:
        await self._run(self._set_topic_state_sync, chat_key(chat), state)

    def _set_topic_state_sync(self, ckey: str, state: str) -> None:
        self._db.execute(
            "UPDATE topics SET state = ?, updated_at = ? WHERE chat_key = ?",
            (state, time.time(), ckey),
        )

    async def remove_topic(self, chat: Any, *,
                           expect_thread_id: int | None = None,
                           expect_revision: int | None = None) -> int | None:
        """Compare-and-delete: drop the mapping only if it is still the one
        the caller saw.

        A stale ``/del`` button pressed after the chat was re-bound used to
        delete the *new* mapping while trying to remove the old thread
        (audit T05).
        """
        return await self._run(self._remove_topic_sync, chat_key(chat),
                               expect_thread_id, expect_revision)

    def _remove_topic_sync(self, ckey: str, expect_thread_id: int | None,
                           expect_revision: int | None) -> int | None:
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT * FROM topics WHERE chat_key = ?",
                             (ckey,)).fetchone()
            if row is None:
                db.execute("COMMIT")
                return None
            if expect_thread_id is not None and row["thread_id"] != expect_thread_id:
                db.execute("COMMIT")
                return None
            if expect_revision is not None and row["revision"] != expect_revision:
                db.execute("COMMIT")
                return None
            db.execute("DELETE FROM topics WHERE chat_key = ?", (ckey,))
            db.execute("COMMIT")
            return int(row["thread_id"])
        except Exception:
            db.execute("ROLLBACK")
            raise

    # ── message map ────────────────────────────────────────────────

    async def link_messages(self, *, account: str, chat: Any, max_id: str,
                            tg_ids: Iterable[int]) -> None:
        """Remember that one MAX message is these Telegram messages.

        ``delivery_parts`` already holds the same facts, but only as a JSON blob
        under a job ID — answering "which MAX message is Telegram message N?"
        meant scanning it. Replies need that question answered on the hot path,
        in both directions, so it gets a real index.
        """
        ids = [int(i) for i in tg_ids if i]
        if not ids or not max_id:
            return
        await self._run(self._link_messages_sync, account, chat_key(chat),
                        str(max_id), ids)

    def _link_messages_sync(self, account: str, ckey: str, max_id: str,
                            tg_ids: list[int]) -> None:
        now = time.time()
        self._db.executemany(
            "INSERT INTO message_map (account, chat_key, max_id, tg_id, created_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(account, chat_key, max_id, tg_id) DO NOTHING",
            [(account, ckey, max_id, tg_id, now) for tg_id in tg_ids],
        )

    async def max_id_for_tg(self, *, account: str, chat: Any,
                            tg_id: int) -> str | None:
        """The MAX message a Telegram message corresponds to, if we know it."""
        return await self._run(self._max_id_for_tg_sync, account,
                               chat_key(chat), int(tg_id))

    def _max_id_for_tg_sync(self, account: str, ckey: str,
                            tg_id: int) -> str | None:
        row = self._db.execute(
            "SELECT max_id FROM message_map"
            " WHERE account = ? AND chat_key = ? AND tg_id = ?"
            " ORDER BY created_at LIMIT 1",
            (account, ckey, tg_id),
        ).fetchone()
        return row["max_id"] if row else None

    async def tg_id_for_max(self, *, account: str, chat: Any,
                            max_id: str) -> int | None:
        """The first Telegram message a MAX message produced, if we know it."""
        return await self._run(self._tg_id_for_max_sync, account,
                               chat_key(chat), str(max_id))

    def _tg_id_for_max_sync(self, account: str, ckey: str,
                            max_id: str) -> int | None:
        row = self._db.execute(
            "SELECT tg_id FROM message_map"
            " WHERE account = ? AND chat_key = ? AND max_id = ?"
            " ORDER BY tg_id LIMIT 1",
            (account, ckey, max_id),
        ).fetchone()
        return int(row["tg_id"]) if row else None

    # ── cursors ────────────────────────────────────────────────────

    async def get_cursor(self, name: str) -> str | None:
        return await self._run(self._get_cursor_sync, name)

    def _get_cursor_sync(self, name: str) -> str | None:
        row = self._db.execute(
            "SELECT value FROM cursors WHERE name = ?", (name,)).fetchone()
        return row["value"] if row else None

    async def set_cursor(self, name: str, value: str) -> None:
        await self._run(self._set_cursor_sync, name, value)

    def _set_cursor_sync(self, name: str, value: str) -> None:
        self._db.execute(
            "INSERT INTO cursors (name, value, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(name) DO UPDATE SET value = excluded.value,"
            " updated_at = excluded.updated_at",
            (name, value, time.time()),
        )

    # ── housekeeping / observability ───────────────────────────────

    async def stats(self, *, source: str | None = None) -> dict[str, Any]:
        return await self._run(self._stats_sync, source)

    def _stats_sync(self, source: str | None) -> dict[str, Any]:
        where, params = ("WHERE source = ?", [source]) if source else ("", [])
        rows = self._db.execute(
            f"SELECT status, COUNT(*) AS n FROM inbox {where} GROUP BY status",
            params).fetchall()
        by_status = {r["status"]: r["n"] for r in rows}
        oldest = self._db.execute(
            f"SELECT MIN(received_at) AS t FROM inbox {where} "
            f"{'AND' if where else 'WHERE'} status IN (?, ?, ?)",
            [*params, STATUS_RECEIVED, STATUS_PROCESSING, STATUS_RETRYABLE],
        ).fetchone()
        oldest_age = None
        if oldest and oldest["t"]:
            oldest_age = max(0.0, time.time() - float(oldest["t"]))
        return {
            "by_status": by_status,
            "pending": sum(by_status.get(s, 0) for s in ACTIVE_STATUSES),
            "failed": by_status.get(STATUS_PERMANENT, 0),
            "uncertain": by_status.get(STATUS_UNCERTAIN, 0),
            "quarantined": by_status.get(STATUS_QUARANTINED, 0),
            "oldest_pending_age": oldest_age,
        }

    async def prune(self) -> int:
        """Drop delivered rows older than the retention window.

        Retention, not a row count, is what bounds the journal: the point of the
        window is that it must comfortably outlive any replay MAX might send
        after an outage (audit D04).
        """
        if self._retention <= 0:
            return 0
        return await self._run(self._prune_sync)

    def _prune_sync(self) -> int:
        cutoff = time.time() - self._retention
        cur = self._db.execute(
            "DELETE FROM inbox WHERE status = ? AND updated_at < ?",
            (STATUS_DELIVERED, cutoff),
        )
        # The reply map is only useful while the messages it points at are still
        # worth quoting, so it ages out on the same window rather than growing
        # for the life of the bridge.
        self._db.execute("DELETE FROM message_map WHERE created_at < ?", (cutoff,))
        return cur.rowcount or 0
