"""TopicService: one creation path, honest titles, safe deletion, JSON import."""

from __future__ import annotations

import asyncio
import json

import pytest
from telegram.error import BadRequest

from app.store import TITLE_PLACEHOLDER, TITLE_RESOLVED
from app.topics import (
    TopicService,
    classify_title,
    is_topic_gone_error,
    migrate_json_topics,
    normalise_title,
)


@pytest.fixture
def topics(store, fake_bot):
    return TopicService(store, fake_bot, -100500)


class TestClassifyTitle:
    def test_a_real_name_is_resolved(self):
        assert classify_title("Алиса") == TITLE_RESOLVED

    def test_empty_is_a_placeholder(self):
        assert classify_title("") == TITLE_PLACEHOLDER

    def test_numeric_id_is_a_placeholder(self):
        assert classify_title("75107924425434") == TITLE_PLACEHOLDER

    def test_negative_numeric_id_is_a_placeholder(self):
        """``isdigit()`` said a negative ID was a real title (audit T07)."""
        assert classify_title("-75107924425434") == TITLE_PLACEHOLDER

    def test_dm_marker_is_a_placeholder(self):
        assert classify_title("DM:12345") == TITLE_PLACEHOLDER


class TestNormaliseTitle:
    def test_falls_back_to_the_chat_id(self):
        assert normalise_title("", -42) == "-42"

    def test_clips_to_telegram_limit(self):
        assert len(normalise_title("x" * 300, 1)) == 128


class TestTopicGoneDetection:
    @pytest.mark.parametrize("text", [
        "Message thread not found", "TOPIC_DELETED", "topic closed",
    ])
    def test_recognised(self, text):
        assert is_topic_gone_error(BadRequest(text)) is True

    def test_other_errors_are_not(self):
        assert is_topic_gone_error(BadRequest("chat not found")) is False


class TestEnsureTopic:
    async def test_creates_once_and_reuses(self, topics, fake_bot):
        first = await topics.ensure_topic(-1, "Alice")
        second = await topics.ensure_topic(-1, "Alice")
        assert first.thread_id == second.thread_id
        assert len(fake_bot.created) == 1
        assert first.created and not second.created

    async def test_concurrent_callers_create_one_topic(self, topics, fake_bot):
        """A MAX message and a /bind used to race into two topics (audit T04)."""
        results = await asyncio.gather(*[topics.ensure_topic(-1, "Alice")
                                         for _ in range(8)])
        assert len({r.thread_id for r in results}) == 1
        assert len(fake_bot.created) == 1

    async def test_creation_failure_returns_an_error_not_general(self, topics,
                                                                 fake_bot):
        """Falling back to General scattered private chats (audit T06)."""
        fake_bot.create_error = BadRequest("not a forum")
        result = await topics.ensure_topic(-1, "Alice")
        assert result.thread_id is None
        assert "not a forum" in result.error

    async def test_placeholder_is_upgraded_to_a_real_name(self, topics, fake_bot):
        await topics.ensure_topic(-1, "-1", TITLE_PLACEHOLDER)
        await topics.ensure_topic(-1, "Алиса", TITLE_RESOLVED)
        assert fake_bot.renamed == [(100, "Алиса")]
        assert (await topics.get_record(-1)).title == "Алиса"

    async def test_a_real_name_is_never_replaced_by_a_placeholder(self, topics,
                                                                  fake_bot):
        await topics.ensure_topic(-1, "Алиса", TITLE_RESOLVED)
        await topics.ensure_topic(-1, "-1", TITLE_PLACEHOLDER)
        assert fake_bot.renamed == []
        assert (await topics.get_record(-1)).title == "Алиса"

    async def test_a_manual_title_is_never_overwritten(self, topics, fake_bot):
        await topics.bind(-1, "Моё название")
        await topics.ensure_topic(-1, "Автоимя", TITLE_RESOLVED)
        assert fake_bot.renamed == []
        assert (await topics.get_record(-1)).title == "Моё название"

    async def test_a_new_chat_title_replaces_an_old_one(self, topics, fake_bot):
        await topics.ensure_topic(-1, "Старое", TITLE_RESOLVED)
        await topics.ensure_topic(-1, "Новое", TITLE_RESOLVED)
        assert fake_bot.renamed == [(100, "Новое")]

    async def test_rename_against_a_deleted_topic_drops_the_mapping(self, topics,
                                                                    fake_bot):
        await topics.ensure_topic(-1, "-1", TITLE_PLACEHOLDER)
        fake_bot.rename_error = BadRequest("message thread not found")
        await topics.ensure_topic(-1, "Алиса", TITLE_RESOLVED)
        assert await topics.get_thread(-1) is None

    async def test_chat_id_type_does_not_create_a_second_topic(self, topics,
                                                               fake_bot):
        await topics.ensure_topic(-1, "Alice")
        await topics.ensure_topic("-1", "Alice")
        assert len(fake_bot.created) == 1


class TestLookup:
    async def test_reverse_lookup(self, topics):
        result = await topics.ensure_topic(-7, "Bob")
        assert await topics.chat_for_thread(result.thread_id) == -7

    async def test_reverse_lookup_of_an_unknown_thread(self, topics):
        assert await topics.chat_for_thread(999) is None

    async def test_rebinding_clears_the_old_thread(self, topics, store):
        first = await topics.ensure_topic(-7, "Bob")
        await store.set_topic(-7, 555, "Bob", TITLE_RESOLVED)
        assert await topics.chat_for_thread(first.thread_id) is None
        assert await topics.chat_for_thread(555) == -7


class TestDeletion:
    async def test_delete_removes_the_topic_and_the_mapping(self, topics, fake_bot):
        result = await topics.ensure_topic(-1, "Alice")
        record = await topics.get_record(-1)
        ok, error = await topics.delete_topic(-1, result.thread_id, record.revision)
        assert ok and error is None
        assert fake_bot.deleted == [result.thread_id]
        assert await topics.get_thread(-1) is None

    async def test_stale_button_deletes_nothing(self, topics, fake_bot, store):
        """A /del button pressed after a re-bind used to delete the new mapping."""
        first = await topics.ensure_topic(-1, "Alice")
        stale_revision = (await topics.get_record(-1)).revision
        await store.set_topic(-1, 777, "Alice", TITLE_RESOLVED)

        ok, error = await topics.delete_topic(-1, first.thread_id, stale_revision)
        assert not ok
        assert "изменилась" in error
        assert fake_bot.deleted == []
        assert await topics.get_thread(-1) == 777

    async def test_failed_delete_keeps_the_mapping(self, topics, fake_bot):
        result = await topics.ensure_topic(-1, "Alice")
        record = await topics.get_record(-1)
        fake_bot.delete_error = BadRequest("not enough rights")
        ok, error = await topics.delete_topic(-1, result.thread_id, record.revision)
        assert not ok and "rights" in error
        assert await topics.get_thread(-1) == result.thread_id

    async def test_already_deleted_topic_releases_the_mapping(self, topics,
                                                              fake_bot):
        result = await topics.ensure_topic(-1, "Alice")
        record = await topics.get_record(-1)
        fake_bot.delete_error = BadRequest("message thread not found")
        ok, _error = await topics.delete_topic(-1, result.thread_id, record.revision)
        assert ok
        assert await topics.get_thread(-1) is None

    async def test_reconcile_missing_drops_a_dead_thread(self, topics):
        result = await topics.ensure_topic(-1, "Alice")
        await topics.reconcile_missing(-1, result.thread_id)
        assert await topics.get_thread(-1) is None

    async def test_reconcile_ignores_a_thread_that_moved_on(self, topics, store):
        await topics.ensure_topic(-1, "Alice")
        await store.set_topic(-1, 999, "Alice", TITLE_RESOLVED)
        await topics.reconcile_missing(-1, 100)
        assert await topics.get_thread(-1) == 999


class TestJsonMigration:
    async def test_imports_and_renames_the_old_file(self, store, tmp_path):
        path = tmp_path / "topics.json"
        path.write_text(json.dumps({"chats": {
            "-1": {"topic_id": 11, "title": "Алиса"},
            "-2": {"topic_id": 12, "title": "-2"},
        }}), encoding="utf-8")

        assert await migrate_json_topics(store, str(path)) == 2
        assert (await store.get_topic(-1)).thread_id == 11
        assert (await store.get_topic(-1)).title_origin == TITLE_RESOLVED
        assert (await store.get_topic(-2)).title_origin == TITLE_PLACEHOLDER
        assert not path.exists()
        assert (tmp_path / "topics.json.imported").exists()

    async def test_runs_only_once(self, store, tmp_path):
        path = tmp_path / "topics.json"
        path.write_text(json.dumps({"chats": {"-1": {"topic_id": 11}}}),
                        encoding="utf-8")
        assert await migrate_json_topics(store, str(path)) == 1
        path.write_text(json.dumps({"chats": {"-9": {"topic_id": 99}}}),
                        encoding="utf-8")
        assert await migrate_json_topics(store, str(path)) == 0

    async def test_missing_file_is_fine(self, store, tmp_path):
        assert await migrate_json_topics(store, str(tmp_path / "nope.json")) == 0

    async def test_duplicate_threads_are_refused_loudly(self, store, tmp_path):
        """Importing either one would route a chat to the wrong topic."""
        path = tmp_path / "topics.json"
        path.write_text(json.dumps({"chats": {
            "-1": {"topic_id": 11}, "-2": {"topic_id": 11},
        }}), encoding="utf-8")
        with pytest.raises(ValueError, match="claimed by both"):
            await migrate_json_topics(store, str(path))

    async def test_malformed_entry_is_refused(self, store, tmp_path):
        path = tmp_path / "topics.json"
        path.write_text(json.dumps({"chats": {"-1": "not an object"}}),
                        encoding="utf-8")
        with pytest.raises(ValueError):
            await migrate_json_topics(store, str(path))

    async def test_corrupt_json_is_refused(self, store, tmp_path):
        path = tmp_path / "topics.json"
        path.write_text("{ broken", encoding="utf-8")
        with pytest.raises(json.JSONDecodeError):
            await migrate_json_topics(store, str(path))


class TestOrphanReporting:
    async def test_completed_creation_leaves_no_orphan(self, topics):
        await topics.ensure_topic(-1, "Alice")
        assert await topics.report_orphans() == []

    async def test_crash_between_create_and_mapping_is_reported(self, topics,
                                                                store):
        """The real orphan case: Telegram made the topic, we never stored it.

        The earlier version of this test wrote the marker back *after* a
        successful binding, so it passed against an implementation that scanned
        the topic table — where, in the situation that actually loses a topic,
        there is nothing to find.
        """
        await store.set_meta("pending_topic:-1", "Alice")
        assert await topics.report_orphans() == ["-1: 'Alice'"]

    async def test_a_chat_whose_topic_never_got_created_is_reported(
            self, topics, fake_bot):
        fake_bot.create_error = RuntimeError("not a forum")
        assert not (await topics.ensure_topic(-1, "Alice")).ok
        assert await topics.report_orphans() == ["-1: 'Alice'"]

    async def test_a_stale_marker_over_a_live_binding_is_cleared(self, topics,
                                                                 store):
        await topics.ensure_topic(-1, "Alice")
        await store.set_meta("pending_topic:-1", "Alice")
        assert await topics.report_orphans() == []
        assert await store.get_meta("pending_topic:-1") is None

    async def test_reporting_survives_a_chat_key_with_a_wildcard(self, topics,
                                                                 store):
        """``LIKE`` treats % and _ as patterns; the prefix must be escaped."""
        await store.set_meta("pending_topic:abc", "Wildcards")
        assert await store.meta_with_prefix("pending_%") == {}
        assert await store.meta_with_prefix("pending_topic:") == {
            "pending_topic:abc": "Wildcards"}
