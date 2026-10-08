"""MAX → Telegram: rendering, album grouping, and the losses that used to happen."""

from __future__ import annotations

import asyncio

import pytest
from telegram.error import Forbidden, NetworkError

from app.errors import Outcome
from app.health import LinkState
from app.max_client import MaxMessage
from app.max_listener import (
    ALBUM_WINDOW_MS,
    MaxToTelegramHandler,
    coordinate,
    file_url,
    human_size,
    meaningful_attaches,
    photo_url,
    render_max_text,
)
from app.net import SpoolFile
from app.pipeline import DeliveryPipeline, RetryPolicy
from app.resolver import ContactResolver
from app.store import PART_CONFIRMED, PART_FAILED, PART_UNCERTAIN, Job
from app.tg_sender import TelegramSender
from app.topics import TopicService
from tests.conftest import drain
from tests.test_tg_sender import FakeBot as FakeSenderBot


class TestRenderMaxText:
    def test_plain_text_is_escaped(self):
        assert render_max_text("<b>hi</b> & bye", []) == "&lt;b&gt;hi&lt;/b&gt; &amp; bye"

    def test_empty_text(self):
        assert render_max_text("", [{"type": "STRONG", "from": 0, "length": 1}]) == ""

    def test_bold(self):
        assert render_max_text("hi bold x", [
            {"type": "STRONG", "from": 3, "length": 4}]) == "hi <b>bold</b> x"

    @pytest.mark.parametrize("max_type,tag", [
        ("STRONG", "b"), ("EMPHASIZED", "i"), ("STRIKETHROUGH", "s"),
        ("UNDERLINE", "u"), ("MONOSPACED", "code"), ("BLOCKQUOTE", "blockquote"),
    ])
    def test_every_supported_style(self, max_type, tag):
        result = render_max_text("abc", [{"type": max_type, "from": 0, "length": 3}])
        assert result == f"<{tag}>abc</{tag}>"

    def test_link_url_is_escaped(self):
        result = render_max_text("go", [{"type": "LINK", "from": 0, "length": 2,
                                         "attributes": {"url": "https://a?b=1&c=2"}}])
        assert result == '<a href="https://a?b=1&amp;c=2">go</a>'

    def test_link_without_a_url_is_ignored(self):
        assert render_max_text("go", [{"type": "LINK", "from": 0, "length": 2}]) == "go"

    def test_nested_ranges_stay_well_formed(self):
        result = render_max_text("abcdef", [
            {"type": "STRONG", "from": 0, "length": 6},
            {"type": "EMPHASIZED", "from": 2, "length": 2}])
        assert result == "<b>ab<i>cd</i>ef</b>"

    def test_overlapping_ranges_are_clamped_not_crossed(self):
        """Crossed tags make Telegram reject the whole message."""
        result = render_max_text("abcdef", [
            {"type": "STRONG", "from": 0, "length": 4},
            {"type": "EMPHASIZED", "from": 2, "length": 4}])
        assert result.count("<b>") == result.count("</b>")
        assert result.index("</i>") < result.index("</b>")

    def test_out_of_range_offsets_are_dropped(self):
        assert render_max_text("abc", [
            {"type": "STRONG", "from": 99, "length": 3}]) == "abc"

    def test_length_beyond_the_end_is_clamped(self):
        assert render_max_text("abc", [
            {"type": "STRONG", "from": 1, "length": 99}]) == "a<b>bc</b>"

    def test_zero_length_is_dropped(self):
        assert render_max_text("abc", [
            {"type": "STRONG", "from": 0, "length": 0}]) == "abc"

    def test_garbage_elements_are_skipped(self):
        assert render_max_text("abc", [
            "nonsense", None, {"type": "STRONG", "from": "x", "length": "y"},
        ]) == "abc"

    def test_unknown_element_type_is_ignored(self):
        assert render_max_text("abc", [
            {"type": "SPARKLES", "from": 0, "length": 3}]) == "abc"

    def test_formatting_and_escaping_together(self):
        result = render_max_text("a<b&", [{"type": "STRONG", "from": 1, "length": 3}])
        assert result == "a<b>&lt;b&amp;</b>"


class TestAttachHelpers:
    def test_control_attaches_are_dropped(self):
        assert meaningful_attaches([
            {"_type": "CONTROL"}, {"_type": "PHOTO"}, {"_type": "WIDGET"},
        ]) == [{"_type": "PHOTO"}]

    def test_non_dict_entries_are_dropped(self):
        assert meaningful_attaches(["x", None, {"_type": "FILE"}]) == [{"_type": "FILE"}]

    def test_none_is_empty(self):
        assert meaningful_attaches(None) == []

    def test_photo_url_prefers_base_url(self):
        assert photo_url({"baseUrl": "https://a/1",
                          "baseRawUrl": "https://a/2"}) == "https://a/1"

    def test_photo_url_falls_back_to_raw(self):
        """``baseRawUrl`` was never tried and those photos failed (audit M09)."""
        assert photo_url({"baseRawUrl": "https://a/raw"}) == "https://a/raw"

    def test_photo_url_without_anything(self):
        assert photo_url({"photoToken": "abc"}) is None

    def test_file_url_requires_http(self):
        assert file_url({"url": "not-a-url"}) is None
        assert file_url({"url": "https://a/f"}) == "https://a/f"


class TestCoordinate:
    def test_zero_is_a_real_coordinate(self):
        """``lat or latitude`` discarded the equator and the prime meridian."""
        assert coordinate(0) == 0.0
        assert coordinate(0.0, 5) == 0.0

    def test_none_falls_through(self):
        assert coordinate(None, 12.5) == 12.5

    def test_strings_are_parsed(self):
        assert coordinate("55.75") == 55.75

    def test_garbage_falls_through(self):
        assert coordinate("nope", 1.0) == 1.0

    def test_nothing_usable(self):
        assert coordinate(None, "x") is None

    def test_booleans_are_not_coordinates(self):
        assert coordinate(True, 3.0) == 3.0


class TestHumanSize:
    def test_bytes(self):
        assert human_size(512) == "512 Б"

    def test_kilobytes(self):
        assert human_size(2048) == "2.0 КБ"

    def test_megabytes(self):
        assert human_size(5 * 1024 * 1024) == "5.0 МБ"


# ── handler-level behaviour ────────────────────────────────────────

class StubResolver(ContactResolver):
    def __init__(self, dm=True, names=None, titles=None):
        super().__init__(client=None)
        self._dm = dm
        self._names_map = names or {}
        self._titles_map = titles or {}

    def is_dm(self, chat_id):
        return self._dm

    async def resolve_user(self, user_id):
        if user_id is None:
            return ""
        return self._names_map.get(user_id, str(user_id))

    def chat_name(self, chat_id):
        return self._titles_map.get(str(chat_id), str(chat_id))

    async def ensure_chat(self, chat_id):
        return {}


def make_job(job_id: int, message: MaxMessage) -> Job:
    return Job(id=job_id, source="max", account="acc", chat_key=message.chat_key,
               event_id=message.message_id, revision=message.revision or "r",
               payload=message.to_payload(), status="received", attempts=0,
               next_retry_at=None, last_error=None, received_at=0.0,
               updated_at=0.0)


def photo_message(mid: str, sender: int = 1, text: str = "",
                  timestamp: int = 1000) -> MaxMessage:
    return MaxMessage(chat_id=-1, sender_id=sender, text=text, message_id=mid,
                      timestamp=timestamp, attaches=[{"_type": "PHOTO",
                                                      "baseUrl": "https://i.oneme.ru/p"}],
                      revision=f"rev{mid}")


@pytest.fixture
def handler(store, fake_bot, health):
    sender = TelegramSender("t", "-100500", bot=FakeSenderBot())
    sender.RETRY_BACKOFF_SEC = 0.0
    topics = TopicService(store, fake_bot, -100500)
    return MaxToTelegramHandler(None, sender, topics, StubResolver(), health)


class TestGrouping:
    async def test_bare_media_from_one_sender_is_grouped(self, handler):
        lead = make_job(1, photo_message("1", text="подпись"))
        rest = [make_job(2, photo_message("2")), make_job(3, photo_message("3"))]
        assert [j.id for j in await handler.group(lead, rest)] == [1, 2, 3]

    async def test_a_second_caption_ends_the_group(self, handler):
        """Two captioned photos are two messages; merging would drop one caption."""
        lead = make_job(1, photo_message("1", text="первая"))
        rest = [make_job(2, photo_message("2", text="вторая"))]
        assert [j.id for j in await handler.group(lead, rest)] == [1]

    async def test_a_different_sender_ends_the_group(self, handler):
        lead = make_job(1, photo_message("1"))
        rest = [make_job(2, photo_message("2", sender=999))]
        assert [j.id for j in await handler.group(lead, rest)] == [1]

    async def test_a_late_message_is_not_part_of_the_album(self, handler):
        lead = make_job(1, photo_message("1", timestamp=1000))
        rest = [make_job(2, photo_message("2", timestamp=1000 + ALBUM_WINDOW_MS + 1))]
        assert [j.id for j in await handler.group(lead, rest)] == [1]

    async def test_text_only_messages_are_never_grouped(self, handler):
        lead = make_job(1, MaxMessage(chat_id=-1, sender_id=1, text="hi",
                                      message_id="1", timestamp=1))
        rest = [make_job(2, photo_message("2"))]
        assert [j.id for j in await handler.group(lead, rest)] == [1]

    async def test_a_reply_is_never_grouped(self, handler):
        message = photo_message("1")
        message.link = {"type": "REPLY", "message": {"id": 9}}
        lead = make_job(1, message)
        assert [j.id for j in await handler.group(lead, [make_job(2, photo_message("2"))])] == [1]

    async def test_missing_timestamps_prevent_grouping(self, handler):
        lead = make_job(1, photo_message("1", timestamp=None))
        rest = [make_job(2, photo_message("2", timestamp=None))]
        assert [j.id for j in await handler.group(lead, rest)] == [1]

    async def test_item_cap_is_respected(self, handler):
        lead = make_job(1, photo_message("1"))
        rest = [make_job(i, photo_message(str(i))) for i in range(2, 60)]
        grouped = await handler.group(lead, rest)
        assert len(grouped) <= 30


class TestDelivery:
    async def test_own_messages_are_not_forwarded(self, handler, store):
        message = MaxMessage(chat_id=-1, sender_id=7, text="mine",
                             message_id="1", is_self=True, revision="r")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        result = await handler.process([job])
        assert result == {job.id: Outcome.DELIVERED}
        assert handler.sender.bot.calls == []

    async def test_text_message_reaches_the_topic(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health,
                                    retry=RetryPolicy(max_attempts=1))
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, text="привет",
                             message_id="1", revision="r")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        result = await handler.process([job])
        assert result[job.id] is Outcome.DELIVERED
        name, kwargs = handler.sender.bot.calls[0]
        assert name == "send_message"
        assert "привет" in kwargs["text"]
        assert kwargs["message_thread_id"] == 100

    async def test_topic_creation_failure_retries_rather_than_using_general(
            self, handler, store, fake_bot, health):
        from telegram.error import BadRequest
        fake_bot.create_error = BadRequest("not a forum")
        message = MaxMessage(chat_id=-1, sender_id=7, text="hi",
                             message_id="1", revision="r")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        assert await handler.process([job]) == {job.id: Outcome.TRANSIENT}
        assert handler.sender.bot.calls == []

    async def test_confirmed_parts_are_not_resent(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health,
                                    retry=RetryPolicy(max_attempts=1))
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, text="привет",
                             message_id="1", revision="r")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        await handler.process([job])
        calls_after_first = len(handler.sender.bot.calls)
        await handler.process([job])
        assert len(handler.sender.bot.calls) == calls_after_first

    async def test_empty_message_still_produces_a_post(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, message_id="1", revision="r")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        assert (await handler.process([job]))[job.id] is Outcome.DELIVERED
        assert "пустое" in handler.sender.bot.calls[0][1]["text"]

    async def test_location_uses_the_native_endpoint(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, message_id="1", revision="r",
                             attaches=[{"_type": "LOCATION", "lat": 0, "lon": 0}])
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        await handler.process([job])
        names = [c[0] for c in handler.sender.bot.calls]
        assert "send_location" in names

    async def test_share_attachment_becomes_text(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, message_id="1", revision="r",
                             attaches=[{"_type": "SHARE", "title": "T",
                                        "url": "https://x/y"}])
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        await handler.process([job])
        assert "https://x/y" in handler.sender.bot.calls[0][1]["text"]


class TestHeaderAndQuote:
    async def test_dm_header_shows_the_sender(self, handler):
        handler.resolver = StubResolver(dm=True, names={7: "Алиса"})
        header = await handler._header(MaxMessage(chat_id=-1, sender_id=7))
        assert header == "✉ <b>Алиса</b>"

    async def test_group_header_shows_chat_and_sender(self, handler):
        handler.resolver = StubResolver(dm=False, names={7: "Боб"},
                                        titles={"-1": "Команда"})
        header = await handler._header(MaxMessage(chat_id=-1, sender_id=7))
        assert header == "💬 <b>Команда</b> | Боб"

    async def test_channel_post_has_no_dangling_separator(self, handler):
        handler.resolver = StubResolver(dm=False, titles={"-1": "Канал"})
        header = await handler._header(MaxMessage(chat_id=-1, sender_id=None))
        assert header == "💬 <b>Канал</b>"

    async def test_reply_quote_shows_a_preview(self, handler):
        handler.resolver = StubResolver(names={9: "Алиса"})
        message = MaxMessage(chat_id=-1, link={
            "type": "REPLY", "message": {"id": 5, "sender": 9,
                                         "text": "исходное сообщение"}})
        quote = await handler._quote_block(message)
        assert "Ответ на Алиса" in quote
        assert "исходное сообщение" in quote

    async def test_forward_quote_names_the_author(self, handler):
        handler.resolver = StubResolver(names={9: "Алиса"})
        message = MaxMessage(chat_id=-1, link={"type": "FORWARD",
                                               "message": {"sender": 9}})
        assert "Переслано от Алиса" in await handler._quote_block(message)

    async def test_no_link_means_no_quote(self, handler):
        assert await handler._quote_block(MaxMessage(chat_id=-1)) == ""

    async def test_quote_of_an_attachment_only_message(self, handler):
        message = MaxMessage(chat_id=-1, link={
            "type": "REPLY", "message": {"id": 5, "attaches": [{"_type": "PHOTO"}]}})
        assert "вложени" in await handler._quote_block(message)

    def test_forwarded_content_is_extracted(self, handler):
        message = MaxMessage(chat_id=-1, link={
            "type": "FORWARD",
            "message": {"id": 3, "text": "оригинал", "sender": 9,
                        "attaches": [{"_type": "PHOTO"}]}})
        inner = handler._forwarded_content(message)
        assert inner.text == "оригинал"
        assert inner.attaches == [{"_type": "PHOTO"}]

    def test_a_reply_is_not_forwarded_content(self, handler):
        message = MaxMessage(chat_id=-1, link={"type": "REPLY",
                                               "message": {"id": 3}})
        assert handler._forwarded_content(message) is None


class TestReplyTargeting:
    async def test_reply_points_at_the_delivered_message(self, handler, store,
                                                         health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        original = await store.enqueue(source="max", account="acc", chat=-1,
                                       event_id="5", revision="r", payload={})
        await store.record_part(original.id, "text", "text", PART_CONFIRMED,
                                destination_ids=[4242])
        message = MaxMessage(chat_id=-1, link={"type": "REPLY",
                                               "message": {"id": 5}})
        assert await handler._reply_target(message) == 4242

    async def test_unknown_original_has_no_reply_target(self, handler, store,
                                                        health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, link={"type": "REPLY",
                                               "message": {"id": 999}})
        assert await handler._reply_target(message) is None

    async def test_forwards_do_not_use_reply_targeting(self, handler):
        message = MaxMessage(chat_id=-1, link={"type": "FORWARD",
                                               "message": {"id": 5}})
        assert await handler._reply_target(message) is None


class TestEdits:
    """A new revision of a known message is an edit, not an unexplained repost."""

    async def test_an_edit_is_marked_and_attached_to_the_original(
            self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)

        original_msg = MaxMessage(chat_id=-1, sender_id=7, text="перв",
                                  message_id="10", revision="r1")
        original = await store.enqueue(source="max", account="acc", chat=-1,
                                       event_id="10", revision="r1",
                                       payload=original_msg.to_payload())
        await handler.process([original])
        first_call = handler.sender.bot.calls[-1]
        original_tg_id = 1

        edited_msg = MaxMessage(chat_id=-1, sender_id=7, text="второй вариант",
                                message_id="10", revision="r2")
        edited = await store.enqueue(source="max", account="acc", chat=-1,
                                     event_id="10", revision="r2",
                                     payload=edited_msg.to_payload())
        await handler.process([edited])

        _name, kwargs = handler.sender.bot.calls[-1]
        assert "изменено" in kwargs["text"]
        assert "второй вариант" in kwargs["text"]
        assert kwargs["reply_to_message_id"] == original_tg_id
        assert "изменено" not in first_call[1]["text"]

    async def test_a_first_delivery_is_not_marked_as_edited(self, handler, store,
                                                            health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, text="текст",
                             message_id="11", revision="r1")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="11", revision="r1",
                                  payload=message.to_payload())
        await handler.process([job])
        assert "изменено" not in handler.sender.bot.calls[-1][1]["text"]

    async def test_an_edit_of_an_undelivered_message_is_not_marked(
            self, handler, store, health):
        """Nothing to attach to means nothing to call an edit."""
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        await store.enqueue(source="max", account="acc", chat=-1,
                            event_id="12", revision="r1", payload={})
        message = MaxMessage(chat_id=-1, sender_id=7, text="правка",
                             message_id="12", revision="r2")
        edited = await store.enqueue(source="max", account="acc", chat=-1,
                                     event_id="12", revision="r2",
                                     payload=message.to_payload())
        await handler.process([edited])
        assert "изменено" not in handler.sender.bot.calls[-1][1]["text"]


class StubDownloadClient:
    """Serves media whose sniffed kind is decided by the URL."""

    def __init__(self, spool):
        self.spool = spool
        self.downloads: list[str] = []

    async def download_media(self, url, *, filename="file", max_bytes=None):
        import os
        self.downloads.append(url)
        if "photo" in url:
            body, ctype = b"\xff\xd8\xff" + b"x" * 32, "image/jpeg"
        else:
            body, ctype = b"%PDF-1.4\n" + b"x" * 32, "application/pdf"
        fd, path = self.spool.new_temp()
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
        await self.spool.reserve(len(body))
        return SpoolFile(path, filename, ctype, len(body), self.spool)

    async def resolve_file_url(self, *args, **kwargs):
        return None

    async def resolve_video_url(self, *args, **kwargs):
        return None


@pytest.fixture
def download_handler(store, fake_bot, health, spool):
    sender = TelegramSender("t", "-100500", bot=FakeSenderBot())
    sender.RETRY_BACKOFF_SEC = 0.0
    topics = TopicService(store, fake_bot, -100500)
    # The stub stands in for a connected, authorised MAX client.
    health.set_state(LinkState.READY)
    return MaxToTelegramHandler(StubDownloadClient(spool), sender, topics,
                                StubResolver(), health)


def file_message(mid: str, sender: int = 1, timestamp: int = 1000) -> MaxMessage:
    return MaxMessage(chat_id=-1, sender_id=sender, text="", message_id=mid,
                      timestamp=timestamp,
                      attaches=[{"_type": "FILE", "name": "doc.pdf",
                                 "url": "https://i.oneme.ru/doc.pdf"}],
                      revision=f"rev{mid}")


class TestMixedAlbumBatch:
    """An album may only carry what Telegram groups — everything else is posted.

    ``sendMediaGroup`` takes photos and videos. A document prepared inside an
    album batch was matched by neither the "groupable" nor the "has a notice"
    branch, so it was dropped on the floor while its job was still recorded as
    delivered: a silent loss, which is the one thing the delivery model exists
    to prevent (audit M03/M06, §13).
    """

    async def _run(self, handler, store, health, messages):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        jobs = []
        for message in messages:
            jobs.append(await store.enqueue(
                source="max", account="acc", chat=-1,
                event_id=message.message_id, revision=message.revision,
                payload=message.to_payload()))
        grouped = await handler.group(jobs[0], jobs[1:])
        outcomes = await handler.process(grouped)
        return grouped, outcomes

    async def test_a_document_beside_a_photo_still_reaches_telegram(
            self, download_handler, store, health):
        handler = download_handler
        photo = photo_message("1", timestamp=1000)
        photo.attaches = [{"_type": "PHOTO", "baseUrl": "https://i.oneme.ru/photo"}]
        grouped, outcomes = await self._run(
            handler, store, health, [photo, file_message("2", timestamp=1001)])

        assert len(grouped) == 2, "precondition: the two messages are batched"
        sent = [name for name, _kwargs in handler.sender.bot.calls]
        assert "send_document" in sent, (
            f"the document never reached Telegram; calls were {sent}")
        assert "send_photo" in sent or "send_media_group" in sent
        assert all(o is Outcome.DELIVERED for o in outcomes.values())

    async def test_each_message_gets_its_own_recorded_part(
            self, download_handler, store, health):
        handler = download_handler
        photo = photo_message("1", timestamp=1000)
        photo.attaches = [{"_type": "PHOTO", "baseUrl": "https://i.oneme.ru/photo"}]
        grouped, _outcomes = await self._run(
            handler, store, health, [photo, file_message("2", timestamp=1001)])
        for job in grouped:
            parts = await store.parts(job.id)
            assert any(p.status == PART_CONFIRMED for p in parts.values()), (
                f"job {job.id} was marked delivered with nothing confirmed")

    async def test_photos_only_still_go_out_as_one_album(
            self, download_handler, store, health):
        handler = download_handler
        first = photo_message("1", timestamp=1000)
        first.attaches = [{"_type": "PHOTO", "baseUrl": "https://i.oneme.ru/photo"}]
        second = photo_message("2", timestamp=1001)
        second.attaches = [{"_type": "PHOTO", "baseUrl": "https://i.oneme.ru/photo"}]
        _grouped, outcomes = await self._run(handler, store, health,
                                             [first, second])
        sent = [name for name, _kwargs in handler.sender.bot.calls]
        assert sent.count("send_media_group") == 1
        assert all(o is Outcome.DELIVERED for o in outcomes.values())

    async def test_one_failed_download_in_a_batch_leaves_that_job_retryable(
            self, download_handler, store, health):
        """A placeholder is not the photo, so the job is not finished.

        When *some* of a batch downloaded, the album went out with what it had
        and every job in it — including the one whose photo never arrived — was
        recorded as delivered, with a note in its place. That retires the job
        for good. The single-message path already got this right (audit M09).
        """
        handler = download_handler
        real_download = handler.client.download_media

        async def flaky_download(url, *, filename="file", max_bytes=None):
            # One of the two photos is unreachable for as long as the outage
            # lasts — retrying it inside the same batch will not help.
            if url.endswith("unreachable"):
                raise OSError("connection reset")
            return await real_download(url, filename=filename,
                                       max_bytes=max_bytes)

        handler.client.download_media = flaky_download

        first = photo_message("1", timestamp=1000)
        first.attaches = [{"_type": "PHOTO",
                           "baseUrl": "https://i.oneme.ru/photo-ok"}]
        second = photo_message("2", timestamp=1001)
        second.attaches = [{"_type": "PHOTO",
                            "baseUrl": "https://i.oneme.ru/photo-unreachable"}]
        grouped, outcomes = await self._run(handler, store, health,
                                            [first, second])
        assert outcomes[grouped[0].id] is Outcome.DELIVERED, outcomes
        assert outcomes[grouped[1].id] is Outcome.TRANSIENT, outcomes


class TestCaptionThatFollowsAnAttachment:
    """Content posted after a successful attachment must not vanish quietly."""

    async def test_a_failed_location_caption_is_not_reported_as_delivered(
            self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, text="я тут",
                             message_id="30", revision="r1",
                             attaches=[{"_type": "LOCATION", "lat": 55.7,
                                        "lon": 37.6}])
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="30", revision="r1",
                                  payload=message.to_payload())
        # The pin goes out; the caption that follows it does not.
        handler.sender.bot.failures = [None, Forbidden("nope")]
        outcomes = await handler.process([job])
        assert outcomes[job.id] is not Outcome.DELIVERED

    async def test_the_pin_itself_is_not_re_sent_on_a_transient_caption_failure(
            self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, text="я тут",
                             message_id="31", revision="r1",
                             attaches=[{"_type": "LOCATION", "lat": 55.7,
                                        "lon": 37.6}])
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="31", revision="r1",
                                  payload=message.to_payload())
        handler.sender.bot.failures = [None] + [NetworkError("blip")] * 3
        outcomes = await handler.process([job])
        assert outcomes[job.id] is Outcome.UNCERTAIN


class TestMessageMapPopulation:
    """Every confirmed delivery makes the message quotable from the other side."""

    async def test_a_delivered_message_is_indexed(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        message = MaxMessage(chat_id=-1, sender_id=7, text="привет",
                             message_id="40", revision="r1")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="40", revision="r1",
                                  payload=message.to_payload())
        await handler.process([job])
        assert await store.max_id_for_tg(account="acc", chat=-1, tg_id=1) == "40"
        assert await store.tg_id_for_max(account="acc", chat=-1, max_id="40") == 1

    async def test_a_failed_delivery_is_not_indexed(self, handler, store, health):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health)
        handler.bind(pipeline)
        handler.sender.bot.failures = [Forbidden("nope")]
        message = MaxMessage(chat_id=-1, sender_id=7, text="привет",
                             message_id="41", revision="r1")
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="41", revision="r1",
                                  payload=message.to_payload())
        await handler.process([job])
        assert await store.tg_id_for_max(account="acc", chat=-1,
                                         max_id="41") is None


class TestSilentCatchUp:
    """History replayed after an outage must not ping the whole group.

    The flag rides in the journal payload, so it survives a restart between
    enqueue and delivery, and it has to reach every Bot API call the delivery
    makes — text, single media and albums alike.
    """

    async def _deliver(self, handler, store, health, messages, *, silent_flags):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health,
                                    retry=RetryPolicy(max_attempts=1))
        handler.bind(pipeline)
        jobs = []
        for message, flag in zip(messages, silent_flags, strict=True):
            payload = message.to_payload()
            if flag:
                payload["silent"] = True
            jobs.append(await store.enqueue(
                source="max", account="acc", chat=-1,
                event_id=message.message_id, revision=message.revision,
                payload=payload))
        grouped = await handler.group(jobs[0], jobs[1:])
        outcomes = await handler.process(grouped)
        assert all(o is Outcome.DELIVERED for o in outcomes.values())
        return [(name, kwargs.get("disable_notification"))
                for name, kwargs in handler.sender.bot.calls]

    async def test_silent_text_does_not_notify(self, handler, store, health):
        message = MaxMessage(chat_id=-1, sender_id=7, text="вчерашнее",
                             message_id="1", revision="r")
        calls = await self._deliver(handler, store, health, [message],
                                    silent_flags=[True])
        assert calls == [("send_message", True)]

    async def test_live_text_still_notifies(self, handler, store, health):
        message = MaxMessage(chat_id=-1, sender_id=7, text="сейчас",
                             message_id="1", revision="r")
        calls = await self._deliver(handler, store, health, [message],
                                    silent_flags=[False])
        assert calls == [("send_message", False)]

    async def test_silent_album_does_not_notify(self, download_handler, store,
                                                health):
        photos = [photo_message("1", timestamp=1000),
                  photo_message("2", timestamp=1001)]
        calls = await self._deliver(download_handler, store, health, photos,
                                    silent_flags=[True, True])
        assert calls, "precondition: something was sent"
        assert all(flag is True for _name, flag in calls), calls

    async def test_a_live_message_in_the_batch_keeps_the_notification(
            self, download_handler, store, health):
        photos = [photo_message("1", timestamp=1000),
                  photo_message("2", timestamp=1001)]
        calls = await self._deliver(download_handler, store, health, photos,
                                    silent_flags=[True, False])
        assert calls and all(flag is False for _name, flag in calls), calls


class TestWaitsForMaxBeforeRendering:
    """On start-up the journal replays before MAX has authorised.

    The first catch-up album of October 2026 went out with raw numeric IDs
    where the chat and sender names belong, because names come from MAX.
    """

    @pytest.fixture
    def connected_handler(self, store, fake_bot, health, spool):
        sender = TelegramSender("t", "-100500", bot=FakeSenderBot())
        sender.RETRY_BACKOFF_SEC = 0.0
        topics = TopicService(store, fake_bot, -100500)
        return MaxToTelegramHandler(StubDownloadClient(spool), sender, topics,
                                    StubResolver(), health)

    async def _job(self, store):
        message = MaxMessage(chat_id=-1, sender_id=7, text="привет",
                             message_id="1", revision="r")
        return await store.enqueue(source="max", account="acc", chat=-1,
                                   event_id="1", revision="r",
                                   payload=message.to_payload())

    async def test_delivery_holds_until_the_link_is_ready(
            self, connected_handler, store, health):
        handler = connected_handler
        job = await self._job(store)
        task = asyncio.create_task(handler.process([job]))
        assert not await drain(task.done, timeout=0.2), \
            "delivered before MAX was ready"
        assert handler.sender.bot.calls == []

        health.set_state(LinkState.READY)
        assert (await asyncio.wait_for(task, 5))[job.id] is Outcome.DELIVERED

    async def test_a_long_outage_does_not_block_delivery(
            self, connected_handler, store, health):
        """Names are nice to have; the message itself must still arrive."""
        handler = connected_handler
        handler.READY_WAIT_SEC = 0.01
        job = await self._job(store)
        result = await handler.process([job])
        assert result[job.id] is Outcome.DELIVERED
        assert handler.sender.bot.calls


class TestOneMessageWithSeveralPhotos:
    """A single MAX message with five photos became five Telegram posts, each
    with its own header — the album path only ran for batches of separate
    messages (found in the October 2026 catch-up)."""

    def _message(self, count: int, **extra) -> MaxMessage:
        return MaxMessage(
            chat_id=-1, sender_id=7, text="В гостях у сказки", message_id="1",
            timestamp=1000, revision="r",
            attaches=[{"_type": "PHOTO", "baseUrl": f"https://i.oneme.ru/p{i}"}
                      for i in range(count)],
            **extra)

    async def _deliver(self, handler, store, health, message):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health,
                                    retry=RetryPolicy(max_attempts=1))
        handler.bind(pipeline)
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id=message.message_id,
                                  revision=message.revision,
                                  payload=message.to_payload())
        outcomes = await handler.process([job])
        return job, outcomes, handler.sender.bot.calls

    async def test_five_photos_are_one_album_with_one_caption(
            self, download_handler, store, health):
        job, outcomes, calls = await self._deliver(
            download_handler, store, health, self._message(5))
        assert outcomes[job.id] is Outcome.DELIVERED
        assert [name for name, _ in calls] == ["send_media_group"], calls
        media = calls[0][1]["media"]
        assert len(media) == 5
        assert "В гостях у сказки" in media[0].caption
        assert all(item.caption is None for item in media[1:])

    async def test_a_reply_with_photos_keeps_its_quote_path(
            self, download_handler, store, health):
        """The album path cannot carry a quote; it stays per attachment."""
        message = self._message(2, link={"type": "REPLY",
                                         "message": {"id": "0", "text": "q"}})
        _job, _outcomes, calls = await self._deliver(
            download_handler, store, health, message)
        assert "send_media_group" not in [name for name, _ in calls]

    async def test_a_single_photo_is_still_a_photo(self, download_handler,
                                                   store, health):
        _job, _outcomes, calls = await self._deliver(
            download_handler, store, health, self._message(1))
        assert [name for name, _ in calls] == ["send_photo"]


class TestFileOverTheLimit:
    """Two 50+ MB lesson videos showed up as "[не удалось скачать вложение]":
    no name, no size, and it read like a glitch a retry would fix."""

    def _video_file(self, size):
        return {"_type": "FILE", "name": "урок-06.mp4",
                "size": size, "fileId": 1001}

    async def test_declared_oversize_is_named_and_not_downloaded(
            self, download_handler):
        handler = download_handler
        msg = MaxMessage(chat_id=-1, sender_id=7, message_id="1")
        ready = await handler._prepare(self._video_file(52213731), msg)
        assert handler.client.downloads == []
        assert ready.outcome is Outcome.PERMANENT
        assert "урок-06.mp4" in ready.notice
        assert "лимит" in ready.notice

    async def test_oversize_found_while_downloading_is_permanent(
            self, download_handler, monkeypatch):
        from app.errors import FileTooLarge
        handler = download_handler

        async def refuse(url, **kwargs):
            raise FileTooLarge("declared size 60000000 exceeds limit", size=60000000)
        monkeypatch.setattr(handler.client, "download_media", refuse)
        ready = await handler._download_item("https://i.oneme.ru/v", "video.mp4",
                                             "video")
        assert ready.outcome is Outcome.PERMANENT
        assert "video.mp4" in ready.notice

    async def test_a_full_spool_is_still_worth_retrying(
            self, download_handler, monkeypatch):
        """The process-wide budget frees up; that is not the file's fault."""
        from app.errors import MediaTooLarge
        handler = download_handler

        async def full(url, **kwargs):
            raise MediaTooLarge("media spool budget exhausted")
        monkeypatch.setattr(handler.client, "download_media", full)
        ready = await handler._download_item("https://i.oneme.ru/p", "a.jpg",
                                             "photo")
        assert ready.outcome is Outcome.TRANSIENT


class TestRetryOfAFailedSingleMessageAlbum:
    """A five-photo message whose album send failed outright was retried as
    five separate posts: any recorded part, even a failed one, used to send
    it down the per-attachment path."""

    def _message(self) -> MaxMessage:
        return MaxMessage(
            chat_id=-1, sender_id=7, text="", message_id="1", timestamp=1000,
            revision="r",
            attaches=[{"_type": "PHOTO", "baseUrl": f"https://i.oneme.ru/p{i}"}
                      for i in range(5)])

    async def _retry_after(self, handler, store, health, status, ids=()):
        pipeline = DeliveryPipeline(store, source="max", account="acc",
                                    handler=handler, health=health,
                                    retry=RetryPolicy(max_attempts=1))
        handler.bind(pipeline)
        message = self._message()
        job = await store.enqueue(source="max", account="acc", chat=-1,
                                  event_id="1", revision="r",
                                  payload=message.to_payload())
        await store.record_part(job.id, "album", "album", status,
                                destination_ids=list(ids))
        await handler.process([job])
        return [name for name, _ in handler.sender.bot.calls]

    async def test_a_failed_album_is_retried_as_an_album(
            self, download_handler, store, health):
        calls = await self._retry_after(download_handler, store, health,
                                        PART_FAILED)
        assert calls == ["send_media_group"]

    async def test_a_part_that_may_have_landed_is_not_reposted_as_an_album(
            self, download_handler, store, health):
        calls = await self._retry_after(download_handler, store, health,
                                        PART_UNCERTAIN)
        assert "send_media_group" not in calls
