"""Telegram delivery: splitting, chunking, error classification, explicit results."""

from __future__ import annotations

import asyncio

import pytest
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut

from app.errors import Outcome, SendResult
from app.tg_sender import (
    MediaItem,
    StatusNotifier,
    TelegramSender,
    _visible_len,
    chunk_media,
    classify_error,
    escape,
    partial_failure,
    split_html,
)


class FakeMessage:
    def __init__(self, message_id: int):
        self.message_id = message_id


class FakeBot:
    """Records calls and replays scripted failures."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.next_id = 1
        self.failures: list[BaseException | None] = []

    def _next(self):
        message = FakeMessage(self.next_id)
        self.next_id += 1
        return message

    async def _record(self, name, kwargs):
        self.calls.append((name, kwargs))
        if self.failures:
            failure = self.failures.pop(0)
            if failure is not None:
                raise failure
        return self._next()

    async def send_message(self, **kwargs):
        return await self._record("send_message", kwargs)

    async def send_photo(self, **kwargs):
        return await self._record("send_photo", kwargs)

    async def send_video(self, **kwargs):
        return await self._record("send_video", kwargs)

    async def send_document(self, **kwargs):
        return await self._record("send_document", kwargs)

    async def send_sticker(self, **kwargs):
        return await self._record("send_sticker", kwargs)

    async def send_voice(self, **kwargs):
        return await self._record("send_voice", kwargs)

    async def send_audio(self, **kwargs):
        return await self._record("send_audio", kwargs)

    async def send_animation(self, **kwargs):
        return await self._record("send_animation", kwargs)

    async def send_location(self, **kwargs):
        return await self._record("send_location", kwargs)

    async def send_contact(self, **kwargs):
        return await self._record("send_contact", kwargs)

    async def send_media_group(self, **kwargs):
        self.calls.append(("send_media_group", kwargs))
        if self.failures:
            failure = self.failures.pop(0)
            if failure is not None:
                raise failure
        return [self._next() for _ in kwargs["media"]]

    async def initialize(self):
        pass

    async def shutdown(self):
        pass


@pytest.fixture
def sender():
    sender = TelegramSender("token", "-100500", bot=FakeBot())
    # Exercise the retry path, not the clock.
    sender.RETRY_BACKOFF_SEC = 0.0
    return sender


def photo(name="p.jpg", size=10):
    return MediaItem(kind="photo", source=b"x" * size, filename=name)


class TestSplitHtml:
    def test_short_text_is_one_part(self):
        assert split_html("hello", 4096) == ["hello"]

    def test_empty_text_is_no_parts(self):
        assert split_html("", 4096) == []

    def test_long_text_is_split_within_the_limit(self):
        parts = split_html("a" * 9000, 4096)
        assert len(parts) == 3
        assert all(_visible_len(p) <= 4096 for p in parts)
        assert "".join(parts) == "a" * 9000

    def test_entities_are_never_cut_in_half(self):
        """Truncating inside ``&amp;`` produced HTML Telegram rejects (audit M07)."""
        text = "&amp;" * 200
        parts = split_html(text, 50)
        assert "".join(parts) == text
        for part in parts:
            assert "&am" not in part.replace("&amp;", "")

    def test_an_entity_counts_as_one_visible_character(self):
        assert _visible_len("&amp;") == 1
        assert _visible_len("a&amp;b") == 3

    def test_open_tags_are_closed_and_reopened(self):
        parts = split_html("<b>" + "x" * 300 + "</b>", 100)
        assert len(parts) > 1
        assert parts[0].startswith("<b>") and parts[0].endswith("</b>")
        assert parts[1].startswith("<b>")

    def test_nested_tags_survive_a_split(self):
        parts = split_html("<b><i>" + "y" * 300 + "</i></b>", 100)
        assert parts[0].endswith("</i></b>")
        assert parts[1].startswith("<b><i>")

    def test_no_part_exceeds_the_visible_limit(self):
        parts = split_html("<b>" + ("word " * 2000) + "</b>", 500)
        for part in parts:
            text_only = part.replace("<b>", "").replace("</b>", "")
            assert _visible_len(text_only) <= 500

    def test_splitting_prefers_a_word_boundary(self):
        parts = split_html("word " * 40, 100)
        assert not parts[0].endswith("wor")

    def test_link_tag_attributes_are_not_split(self):
        text = '<a href="https://example.com/x">' + "z" * 300 + "</a>"
        parts = split_html(text, 100)
        for part in parts:
            assert part.count("<a href=") == part.count("</a>")


class TestChunkMedia:
    def test_small_album_stays_whole(self):
        assert [len(c) for c in chunk_media(list(range(5)))] == [5]

    def test_exactly_ten_stays_whole(self):
        assert [len(c) for c in chunk_media(list(range(10)))] == [10]

    def test_eleven_is_balanced_not_ten_plus_one(self):
        """A trailing one-item media group is rejected outright (audit M03)."""
        sizes = [len(c) for c in chunk_media(list(range(11)))]
        assert sizes == [6, 5]
        assert 1 not in sizes

    def test_twenty_one_never_leaves_a_lone_item(self):
        sizes = [len(c) for c in chunk_media(list(range(21)))]
        assert sum(sizes) == 21
        assert 1 not in sizes

    @pytest.mark.parametrize("count", list(range(2, 40)))
    def test_no_chunk_is_ever_alone(self, count):
        sizes = [len(c) for c in chunk_media(list(range(count)))]
        assert sum(sizes) == count
        assert all(2 <= s <= 10 for s in sizes)

    def test_empty_album_is_no_chunks(self):
        assert chunk_media([]) == []


class TestClassifyError:
    def test_retry_after_is_transient_with_a_delay(self):
        result = classify_error(RetryAfter(7))
        assert result.outcome is Outcome.TRANSIENT
        assert result.retry_after == 7

    def test_timeout_is_uncertain(self):
        """Telegram may have accepted it; a blind resend duplicates (audit D06)."""
        assert classify_error(TimedOut()).outcome is Outcome.UNCERTAIN

    def test_forbidden_is_permanent(self):
        assert classify_error(Forbidden("blocked")).outcome is Outcome.PERMANENT

    def test_bad_request_is_permanent(self):
        assert classify_error(BadRequest("bad")).outcome is Outcome.PERMANENT

    def test_missing_topic_is_permanent(self):
        result = classify_error(BadRequest("Message thread not found"))
        assert result.outcome is Outcome.PERMANENT

    def test_network_error_is_transient(self):
        assert classify_error(NetworkError("down")).outcome is Outcome.TRANSIENT

    def test_unknown_error_is_transient(self):
        assert classify_error(ValueError("?")).outcome is Outcome.TRANSIENT


class TestSendText:
    async def test_success_returns_confirmed_ids(self, sender):
        result = await sender.send_text("hello", thread_id=5)
        assert result.ok
        assert result.message_ids == [1]
        assert sender.bot.calls[0][1]["message_thread_id"] == 5

    async def test_empty_text_is_a_no_op(self, sender):
        result = await sender.send_text("")
        assert result.ok and result.message_ids == []
        assert sender.bot.calls == []

    async def test_long_text_becomes_several_messages(self, sender):
        result = await sender.send_text("a" * 9000)
        assert len(result.message_ids) == 3
        assert len(sender.bot.calls) == 3

    async def test_failure_is_reported_not_swallowed(self, sender):
        """The old sender returned None here and the caller logged success."""
        sender.bot.failures = [Forbidden("no")]
        result = await sender.send_text("hi")
        assert not result.ok
        assert result.outcome is Outcome.PERMANENT
        assert "no" in result.error

    async def test_partial_delivery_reports_what_landed(self, sender):
        sender.bot.failures = [None, Forbidden("no")]
        result = await sender.send_text("a" * 9000)
        assert not result.ok
        assert result.message_ids == [1]      # the first part did arrive

    async def test_transient_failure_is_retried(self, sender):
        sender.bot.failures = [NetworkError("blip"), None]
        result = await sender.send_text("hi")
        assert result.ok
        assert len(sender.bot.calls) == 2

    async def test_timeout_is_not_retried(self, sender):
        sender.bot.failures = [TimedOut()]
        result = await sender.send_text("hi")
        assert result.outcome is Outcome.UNCERTAIN
        assert len(sender.bot.calls) == 1


class TestSendMedia:
    async def test_photo_with_caption(self, sender):
        result = await sender.send_media(photo(), thread_id=3, caption="cap")
        assert result.ok
        name, kwargs = sender.bot.calls[0]
        assert name == "send_photo"
        assert kwargs["caption"] == "cap"

    async def test_sticker_is_sent_without_a_caption_argument(self, sender):
        item = MediaItem(kind="sticker", source=b"x", filename="s.webp")
        await sender.send_media(item, thread_id=1)
        name, kwargs = sender.bot.calls[0]
        assert name == "send_sticker"
        assert "caption" not in kwargs

    async def test_overlong_caption_continues_in_a_second_message(self, sender):
        result = await sender.send_media(photo(), caption="c" * 2000)
        assert [c[0] for c in sender.bot.calls] == ["send_photo", "send_message"]
        assert len(result.message_ids) == 2

    async def test_retry_rebuilds_the_payload(self, sender):
        """A consumed BytesIO cannot be read twice; each attempt needs a fresh one."""
        sender.bot.failures = [NetworkError("blip"), None]
        result = await sender.send_media(photo())
        assert result.ok
        first, second = sender.bot.calls
        assert first[1]["photo"] is not second[1]["photo"]


class TestSendAlbum:
    async def test_single_item_uses_the_plain_endpoint(self, sender):
        result = await sender.send_album([photo()], thread_id=1, caption="c")
        assert sender.bot.calls[0][0] == "send_photo"
        assert result.ok

    async def test_eleven_items_send_two_balanced_groups(self, sender):
        result = await sender.send_album([photo(f"{i}.jpg") for i in range(11)])
        groups = [c for c in sender.bot.calls if c[0] == "send_media_group"]
        assert [len(c[1]["media"]) for c in groups] == [6, 5]
        assert len(result.message_ids) == 11

    async def test_caption_rides_only_on_the_first_item(self, sender):
        await sender.send_album([photo(f"{i}.jpg") for i in range(11)],
                                caption="only once")
        captions = []
        for name, kwargs in sender.bot.calls:
            if name == "send_media_group":
                captions.extend(m.caption for m in kwargs["media"])
        assert captions.count("only once") == 1

    async def test_failed_second_chunk_keeps_the_first_chunk_ids(self, sender):
        sender.bot.failures = [None, Forbidden("no")]
        result = await sender.send_album([photo(f"{i}.jpg") for i in range(11)])
        assert not result.ok
        assert len(result.message_ids) == 6

    async def test_empty_album_is_a_no_op(self, sender):
        result = await sender.send_album([])
        assert result.ok and sender.bot.calls == []


class TestStatusNotifier:
    async def test_messages_are_queued_not_awaited(self, sender):
        notifier = StatusNotifier(sender, chat_id=777)
        notifier.start()
        try:
            notifier.notify("hello")
            for _ in range(100):
                if sender.bot.calls:
                    break
                await asyncio.sleep(0.01)
            assert sender.bot.calls[0][1]["chat_id"] == 777
        finally:
            await notifier.stop()

    async def test_notify_never_raises_when_the_queue_is_full(self, sender):
        notifier = StatusNotifier(sender, chat_id=1, queue_size=1)
        notifier.notify("a")
        notifier.notify("b")      # dropped, but must not raise

    def test_incident_repeats_are_throttled(self, sender):
        notifier = StatusNotifier(sender, chat_id=1)
        assert notifier.notify_incident("first") is True
        assert notifier.notify_incident("second") is False

    def test_resolving_an_incident_resets_the_throttle(self, sender):
        """The old counter never reset, so warnings died out permanently (R10)."""
        notifier = StatusNotifier(sender, chat_id=1)
        notifier.notify_incident("down")
        notifier.resolve_incident("up")
        assert notifier.notify_incident("down again") is True

    def test_resolve_without_an_incident_says_nothing(self, sender):
        notifier = StatusNotifier(sender, chat_id=1)
        notifier.resolve_incident("up")
        assert notifier._queue.qsize() == 0


class TestEscape:
    def test_angle_brackets_and_ampersands(self):
        assert escape("<b>&") == "&lt;b&gt;&amp;"

    def test_none_becomes_empty(self):
        assert escape(None) == ""

    def test_numbers_are_stringified(self):
        assert escape(-42) == "-42"


class TestPartialDelivery:
    """A send that half-succeeded must not look like a retryable failure.

    Telegram has no idempotency key for sendMessage, so re-running a send whose
    first half it already accepted posts that half twice. The audit is explicit
    that a partially delivered batch is never re-sent whole (D06/M03, §13);
    these cover every place the sender can stop in the middle.
    """

    def test_nothing_confirmed_keeps_the_original_outcome(self):
        result = partial_failure(SendResult.transient("blip"), [])
        assert result.outcome is Outcome.TRANSIENT

    def test_a_transient_failure_after_a_confirmed_part_is_uncertain(self):
        result = partial_failure(SendResult.transient("blip"), [7])
        assert result.outcome is Outcome.UNCERTAIN
        assert result.message_ids == [7]

    def test_a_permanent_failure_stays_permanent(self):
        result = partial_failure(SendResult.permanent("nope"), [7])
        assert result.outcome is Outcome.PERMANENT
        assert result.message_ids == [7]

    def test_ids_are_not_duplicated(self):
        failed = SendResult(Outcome.TRANSIENT, [7, 8], "blip")
        assert partial_failure(failed, [7]).message_ids == [7, 8]

    async def test_long_text_that_fails_midway_is_not_retried_blind(self, sender):
        sender.bot.failures = [None, NetworkError("blip"), NetworkError("blip"),
                               NetworkError("blip")]
        result = await sender.send_text("a" * 9000)
        assert result.outcome is Outcome.UNCERTAIN
        assert result.message_ids == [1]

    async def test_a_failed_second_album_chunk_is_not_retried_blind(self, sender):
        sender.bot.failures = [None, NetworkError("blip"), NetworkError("blip"),
                               NetworkError("blip")]
        result = await sender.send_album([photo(f"{i}.jpg") for i in range(11)])
        assert result.outcome is Outcome.UNCERTAIN
        assert len(result.message_ids) == 6

    async def test_a_failed_caption_tail_does_not_re_upload_the_media(self, sender):
        sender.bot.failures = [None, NetworkError("blip"), NetworkError("blip"),
                               NetworkError("blip")]
        result = await sender.send_media(photo(), caption="c" * 2000)
        assert result.outcome is Outcome.UNCERTAIN
        assert result.message_ids == [1]

    async def test_a_wholly_failed_send_stays_retryable(self, sender):
        """Nothing landed, so nothing can be duplicated — keep retrying."""
        sender.bot.failures = [NetworkError("blip")] * 3
        result = await sender.send_text("short")
        assert result.outcome is Outcome.TRANSIENT


class TestGroupPacer:
    """Telegram allows ~20 messages a minute per group. A catch-up of a hundred
    messages hit flood control within seconds, and the slow answers that come
    with it timed out into UNCERTAIN jobs."""

    async def test_a_burst_within_the_bucket_is_not_delayed(self):
        from app.tg_sender import _GroupPacer
        pacer = _GroupPacer(per_minute=60, burst=5)
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(5):
            await pacer.acquire()
        assert loop.time() - start < 0.05

    async def test_a_backlog_is_spread_out(self):
        from app.tg_sender import _GroupPacer
        pacer = _GroupPacer(per_minute=1200, burst=2)  # 20/s once drained
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(6):
            await pacer.acquire()
        assert loop.time() - start >= 0.18

    async def test_an_album_costs_one_token_per_item(self):
        from app.tg_sender import _GroupPacer
        pacer = _GroupPacer(per_minute=600, burst=10)
        await pacer.acquire(cost=10)
        loop = asyncio.get_running_loop()
        start = loop.time()
        await pacer.acquire()
        assert loop.time() - start >= 0.08

    def test_the_real_bot_gets_patient_timeouts(self):
        """The 5 s library default turned flood-control slowness into
        UNCERTAIN jobs that need a manual /retry."""
        sender = TelegramSender("123:abc", "-100500")
        request = sender.bot._request[1]
        assert request.read_timeout >= 30


class TestAlbumWireFormat:
    """Every album failed in production with "Can't parse inputmedia: media
    not found": the files had no attach name. A fake bot that records kwargs
    never noticed, so this checks what the real library would put on the wire.
    """

    async def test_every_album_item_is_attached_under_its_own_name(self):
        bot = FakeBot()
        sender = TelegramSender("t", "-100500", bot=bot)
        items = [MediaItem("photo", b"x" * 10, f"{i}.jpg") for i in range(3)]
        result = await sender.send_album(items, 7, caption="c")
        assert result.ok
        (name, kwargs), = bot.calls
        assert name == "send_media_group"
        uris = [m.media.attach_uri for m in kwargs["media"]]
        assert all(uris), uris
        assert len(set(uris)) == 3


class TestReplyToADeletedMessage:
    """A reply whose original was deleted in Telegram failed permanently with
    "Message to be replied not found" and never arrived."""

    async def test_text_reply_survives_a_missing_original(self):
        bot = FakeBot()
        sender = TelegramSender("t", "-100500", bot=bot)
        await sender.send_text("ответ", 7, reply_to=1053)
        (_name, kwargs), = bot.calls
        assert kwargs["reply_to_message_id"] == 1053
        assert kwargs["allow_sending_without_reply"] is True

    async def test_media_reply_survives_a_missing_original(self):
        bot = FakeBot()
        sender = TelegramSender("t", "-100500", bot=bot)
        await sender.send_media(MediaItem("photo", b"x", "a.jpg"), 7,
                                reply_to=1053)
        (_name, kwargs), = bot.calls
        assert kwargs["allow_sending_without_reply"] is True
