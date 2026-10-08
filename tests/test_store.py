"""The delivery journal: dedup keys, lifecycle, topic uniqueness, retention."""

from __future__ import annotations

import time

import pytest

from app.errors import StoreCorrupted
from app.store import (
    PART_CONFIRMED,
    STATUS_DELIVERED,
    STATUS_PERMANENT,
    STATUS_PROCESSING,
    STATUS_RETRYABLE,
    Store,
    chat_key,
    revision_hash,
)


class TestChatKey:
    def test_int_and_string_are_the_same_chat(self):
        assert chat_key(-42) == chat_key("-42") == "-42"

    def test_whitespace_is_stripped(self):
        assert chat_key("  -42 ") == "-42"

    def test_non_numeric_survives_as_text(self):
        assert chat_key("abc") == "abc"

    def test_none_is_empty(self):
        assert chat_key(None) == ""


class TestRevisionHash:
    def test_same_content_same_hash(self):
        assert revision_hash({"a": 1, "b": 2}) == revision_hash({"b": 2, "a": 1})

    def test_different_content_differs(self):
        assert revision_hash({"text": "hi"}) != revision_hash({"text": "hi!"})


class TestDedup:
    async def test_same_event_twice_is_a_replay(self, store):
        first = await store.enqueue(source="max", account="acc", chat=-1,
                                    event_id="m1", revision="r1", payload={})
        second = await store.enqueue(source="max", account="acc", chat=-1,
                                     event_id="m1", revision="r1", payload={})
        assert first is not None
        assert second is None

    async def test_same_id_in_two_chats_is_two_events(self, store):
        """MAX message IDs were treated as globally unique; they are not."""
        a = await store.enqueue(source="max", account="acc", chat=-1,
                                event_id="m1", revision="r", payload={})
        b = await store.enqueue(source="max", account="acc", chat=-2,
                                event_id="m1", revision="r", payload={})
        assert a is not None and b is not None
        assert a.id != b.id

    async def test_same_id_new_revision_is_an_edit(self, store):
        original = await store.enqueue(source="max", account="acc", chat=-1,
                                       event_id="m1", revision="r1", payload={})
        edited = await store.enqueue(source="max", account="acc", chat=-1,
                                     event_id="m1", revision="r2", payload={})
        assert edited is not None and edited.id != original.id

    async def test_two_accounts_are_separate_namespaces(self, store):
        a = await store.enqueue(source="max", account="one", chat=-1,
                                event_id="m", revision="r", payload={})
        b = await store.enqueue(source="max", account="two", chat=-1,
                                event_id="m", revision="r", payload={})
        assert a is not None and b is not None

    async def test_chat_id_type_does_not_split_the_namespace(self, store):
        await store.enqueue(source="max", account="acc", chat=-1,
                            event_id="m", revision="r", payload={})
        again = await store.enqueue(source="max", account="acc", chat="-1",
                                    event_id="m", revision="r", payload={})
        assert again is None


class TestJobLifecycle:
    async def test_next_job_is_the_oldest(self, store):
        first = await store.enqueue(source="max", account="a", chat=1,
                                    event_id="1", revision="r", payload={})
        await store.enqueue(source="max", account="a", chat=1,
                            event_id="2", revision="r", payload={})
        nxt = await store.next_job(source="max", account="a", chat=1)
        assert nxt.id == first.id

    async def test_delivered_jobs_are_not_returned(self, store):
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.finish_job(job.id, STATUS_DELIVERED)
        assert await store.next_job(source="max", account="a", chat=1) is None

    async def test_retry_backoff_is_honoured(self, store):
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.finish_job(job.id, STATUS_RETRYABLE,
                               next_retry_at=time.time() + 60)
        assert await store.next_job(source="max", account="a", chat=1) is None
        later = await store.next_job(source="max", account="a", chat=1,
                                     now=time.time() + 61)
        assert later.id == job.id

    async def test_attempts_increment_on_claim(self, store):
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.mark_processing(job.id)
        await store.mark_processing(job.id)
        assert (await store.get_job(job.id)).attempts == 2

    async def test_crash_recovery_returns_processing_jobs(self, store):
        """A job interrupted mid-delivery must come back, not vanish."""
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.mark_processing(job.id)
        assert (await store.get_job(job.id)).status == STATUS_PROCESSING

        recovered = await store.recover_processing(source="max", account="a")
        assert recovered == 1
        assert (await store.get_job(job.id)).status == STATUS_RETRYABLE
        assert await store.next_job(source="max", account="a", chat=1) is not None

    async def test_requeue_failed(self, store):
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.finish_job(job.id, STATUS_PERMANENT, error="nope")
        assert await store.requeue_failed(source="max", account="a") == 1
        assert (await store.get_job(job.id)).status == STATUS_RETRYABLE

    async def test_active_chats_lists_only_unfinished_work(self, store):
        a = await store.enqueue(source="max", account="a", chat=1,
                                event_id="1", revision="r", payload={})
        await store.enqueue(source="max", account="a", chat=2,
                            event_id="1", revision="r", payload={})
        await store.finish_job(a.id, STATUS_DELIVERED)
        assert await store.active_chats(source="max", account="a") == ["2"]

    async def test_job_history_returns_prior_revisions(self, store):
        first = await store.enqueue(source="max", account="a", chat=1,
                                    event_id="m", revision="r1", payload={})
        second = await store.enqueue(source="max", account="a", chat=1,
                                     event_id="m", revision="r2", payload={})
        history = await store.job_history(source="max", account="a", chat=1,
                                          event_id="m", before_id=second.id)
        assert [j.id for j in history] == [first.id]


class TestParts:
    async def test_confirmed_part_survives_a_retry(self, store):
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.record_part(job.id, "album", "album", "confirmed",
                                destination_ids=[7, 8])
        parts = await store.parts(job.id)
        assert parts["album"].destination_ids == [7, 8]

    async def test_part_is_upserted_not_duplicated(self, store):
        job = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="1", revision="r", payload={})
        await store.record_part(job.id, "text", "text", "failed", error="boom")
        await store.record_part(job.id, "text", "text", "confirmed",
                                destination_ids=[1])
        parts = await store.parts(job.id)
        assert len(parts) == 1
        assert parts["text"].status == "confirmed"


class TestTopics:
    async def test_roundtrip(self, store):
        await store.set_topic(-42, 100, "Alice", "resolved")
        record = await store.get_topic("-42")
        assert record.thread_id == 100
        assert record.chat_id == -42

    async def test_rebinding_clears_the_old_reverse_entry(self, store):
        """Two threads must never both point at one MAX chat."""
        await store.set_topic(-42, 100, "Alice", "resolved")
        await store.set_topic(-42, 200, "Alice", "resolved")
        assert await store.topic_by_thread(100) is None
        assert (await store.topic_by_thread(200)).chat_id == -42

    async def test_thread_collision_evicts_the_previous_owner(self, store):
        await store.set_topic(-1, 100, "A", "resolved")
        await store.set_topic(-2, 100, "B", "resolved")
        assert await store.get_topic(-1) is None
        assert (await store.get_topic(-2)).thread_id == 100

    async def test_revision_advances_on_rebind(self, store):
        first = await store.set_topic(-1, 100, "A", "resolved")
        second = await store.set_topic(-1, 200, "A", "resolved")
        assert second.revision == first.revision + 1

    async def test_compare_and_delete_refuses_a_stale_thread(self, store):
        await store.set_topic(-1, 100, "A", "resolved")
        await store.set_topic(-1, 200, "A", "resolved")
        assert await store.remove_topic(-1, expect_thread_id=100) is None
        assert await store.get_topic(-1) is not None

    async def test_compare_and_delete_refuses_a_stale_revision(self, store):
        record = await store.set_topic(-1, 100, "A", "resolved")
        await store.set_topic(-1, 100, "A", "resolved")   # revision bumps
        assert await store.remove_topic(-1, expect_thread_id=100,
                                        expect_revision=record.revision) is None

    async def test_delete_with_matching_expectations(self, store):
        record = await store.set_topic(-1, 100, "A", "resolved")
        freed = await store.remove_topic(-1, expect_thread_id=100,
                                         expect_revision=record.revision)
        assert freed == 100
        assert await store.get_topic(-1) is None


class TestManifest:
    async def test_binding_to_a_group_is_remembered(self, store):
        await store.check_manifest(account="", group_id="-100500")
        await store.check_manifest(account="", group_id="-100500")

    async def test_different_group_is_refused(self, store):
        """Reusing state against another supergroup would reuse its thread IDs."""
        await store.check_manifest(account="", group_id="-100500")
        with pytest.raises(StoreCorrupted, match="Telegram group"):
            await store.check_manifest(account="", group_id="-100999")

    async def test_different_account_is_refused(self, store):
        await store.check_manifest(account="user-1", group_id="-1")
        with pytest.raises(StoreCorrupted, match="MAX account"):
            await store.check_manifest(account="user-2", group_id="-1")


class TestRetentionAndStats:
    async def test_prune_keeps_undelivered_rows(self, tmp_path):
        db = Store(str(tmp_path / "x.sqlite3"), synchronous="OFF",
                   retention_days=0.00001)
        await db.open()
        try:
            pending = await db.enqueue(source="max", account="a", chat=1,
                                       event_id="p", revision="r", payload={})
            done = await db.enqueue(source="max", account="a", chat=1,
                                    event_id="d", revision="r", payload={})
            await db.finish_job(done.id, STATUS_DELIVERED)
            await db._run(lambda: db._db.execute(
                "UPDATE inbox SET updated_at = 0 WHERE id = ?", (done.id,)))
            assert await db.prune() == 1
            assert await db.get_job(pending.id) is not None
            assert await db.get_job(done.id) is None
        finally:
            await db.close()

    async def test_stats_report_pending_and_failed(self, store):
        await store.enqueue(source="max", account="a", chat=1,
                            event_id="1", revision="r", payload={})
        bad = await store.enqueue(source="max", account="a", chat=1,
                                  event_id="2", revision="r", payload={})
        await store.finish_job(bad.id, STATUS_PERMANENT, error="x")
        stats = await store.stats(source="max")
        assert stats["pending"] == 1
        assert stats["failed"] == 1
        assert stats["oldest_pending_age"] is not None


class TestOpenFailures:
    async def test_a_non_database_file_is_refused(self, tmp_path):
        """A corrupt state file must not be silently replaced with an empty one."""
        path = tmp_path / "broken.sqlite3"
        path.write_bytes(b"this is definitely not sqlite" * 100)
        db = Store(str(path))
        with pytest.raises(StoreCorrupted):
            await db.open()


class TestMessageMap:
    """Which MAX message is Telegram message N, and the reverse.

    ``delivery_parts`` holds the same facts, but as a JSON blob keyed by job ID,
    so answering either question meant scanning. Replies need it answered on the
    hot path in both directions, hence a real index (audit M11).
    """

    async def test_a_link_resolves_both_ways(self, store):
        await store.link_messages(account="acc", chat=-1, max_id="m1",
                                  tg_ids=[100])
        assert await store.max_id_for_tg(account="acc", chat=-1, tg_id=100) == "m1"
        assert await store.tg_id_for_max(account="acc", chat=-1, max_id="m1") == 100

    async def test_one_max_message_may_be_several_telegram_messages(self, store):
        """An album or a split long text produces more than one."""
        await store.link_messages(account="acc", chat=-1, max_id="m1",
                                  tg_ids=[100, 101, 102])
        for tg_id in (100, 101, 102):
            assert await store.max_id_for_tg(
                account="acc", chat=-1, tg_id=tg_id) == "m1"
        # The reverse answer is the first one, which is what a reply quotes.
        assert await store.tg_id_for_max(account="acc", chat=-1, max_id="m1") == 100

    async def test_recording_twice_is_idempotent(self, store):
        await store.link_messages(account="acc", chat=-1, max_id="m1", tg_ids=[100])
        await store.link_messages(account="acc", chat=-1, max_id="m1", tg_ids=[100])
        assert await store.tg_id_for_max(account="acc", chat=-1, max_id="m1") == 100

    async def test_chat_id_type_does_not_split_the_map(self, store):
        await store.link_messages(account="acc", chat="-1", max_id="m1",
                                  tg_ids=[100])
        assert await store.max_id_for_tg(account="acc", chat=-1, tg_id=100) == "m1"

    async def test_chats_do_not_share_telegram_ids(self, store):
        await store.link_messages(account="acc", chat=-1, max_id="m1", tg_ids=[100])
        assert await store.max_id_for_tg(account="acc", chat=-2, tg_id=100) is None

    async def test_accounts_do_not_share_the_map(self, store):
        await store.link_messages(account="a", chat=-1, max_id="m1", tg_ids=[100])
        assert await store.max_id_for_tg(account="b", chat=-1, tg_id=100) is None

    async def test_an_unknown_message_is_none(self, store):
        assert await store.max_id_for_tg(account="acc", chat=-1, tg_id=7) is None
        assert await store.tg_id_for_max(account="acc", chat=-1, max_id="x") is None

    async def test_empty_input_records_nothing(self, store):
        await store.link_messages(account="acc", chat=-1, max_id="m1", tg_ids=[])
        await store.link_messages(account="acc", chat=-1, max_id="", tg_ids=[1])
        assert await store.tg_id_for_max(account="acc", chat=-1, max_id="m1") is None

    async def test_the_map_ages_out_with_the_journal(self, tmp_path):
        """It is only useful while the messages are still worth quoting."""
        from app.store import Store

        db = Store(str(tmp_path / "old.sqlite3"), synchronous="OFF",
                   retention_days=0.5)
        await db.open()
        try:
            await db.link_messages(account="acc", chat=-1, max_id="m1",
                                   tg_ids=[100])
            await db._run(
                db._db.execute,
                "UPDATE message_map SET created_at = 0")
            await db.prune()
            assert await db.max_id_for_tg(
                account="acc", chat=-1, tg_id=100) is None
        finally:
            await db.close()


class TestSchemaUpgrade:
    """An existing state file gains the new table without being rebuilt.

    There is a live deployment carrying schema 1. Every table is created with
    ``IF NOT EXISTS``, so opening an older file adds what is missing and leaves
    the journal alone — but that is a claim about somebody's real data, so it
    gets a test rather than a comment.
    """

    async def test_a_schema_1_file_opens_and_keeps_its_journal(self, tmp_path):
        from app.store import SCHEMA_VERSION, Store

        path = str(tmp_path / "old.sqlite3")
        db = Store(path, synchronous="OFF")
        await db.open()
        try:
            job = await db.enqueue(source="max", account="acc", chat=-1,
                                   event_id="1", revision="r1",
                                   payload={"text": "до апгрейда"})
            await db.record_part(job.id, "text", "text", PART_CONFIRMED,
                                 destination_ids=[55])
            await db.set_topic(-1, 500, "Алиса")
            # Wind it back to what a schema-1 file actually looks like.
            await db._run(lambda: db._db.execute("DROP TABLE message_map"))
            await db._run(lambda: db._db.execute(
                "UPDATE meta SET value = '1' WHERE key = 'schema_version'"))
        finally:
            await db.close()

        upgraded = Store(path, synchronous="OFF")
        await upgraded.open()
        try:
            assert await upgraded.get_meta("schema_version") == str(SCHEMA_VERSION)
            # The journal, its parts and the topic map all survived.
            job = await upgraded.find_job(source="max", account="acc", chat=-1,
                                          event_id="1")
            assert job is not None
            assert job.payload["text"] == "до апгрейда"
            assert (await upgraded.parts(job.id))["text"].destination_ids == [55]
            assert (await upgraded.get_topic(-1)).thread_id == 500
            # And the new table is usable.
            await upgraded.link_messages(account="acc", chat=-1, max_id="1",
                                         tg_ids=[55])
            assert await upgraded.max_id_for_tg(
                account="acc", chat=-1, tg_id=55) == "1"
        finally:
            await upgraded.close()
