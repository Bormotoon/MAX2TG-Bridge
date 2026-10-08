"""Telegram ingress: fail-closed authorisation, link parsing, entity mapping."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from telegram.constants import MessageEntityType

from app.health import Health
from app.tg_handler import (
    Bridge,
    _describe_media,
    authorize,
    entities_to_max_elements,
    parse_max_chat_id,
    parse_max_invite,
    utf16_to_char_offset,
)

SUPERGROUP = -1001234567890


def make_bridge(allowed=frozenset(), admins=frozenset(), allow_any=False):
    return Bridge(client=None, resolver=None, topics=None, health=Health(),
                  supergroup_id=SUPERGROUP, allowed_user_ids=allowed,
                  admin_user_ids=admins, allow_any_member=allow_any)


@dataclass
class FakeChat:
    id: int


@dataclass
class FakeUser:
    id: int
    is_bot: bool = False


class FakeMessage:
    def __init__(self, sender_chat=None):
        self.sender_chat = sender_chat


class FakeUpdate:
    def __init__(self, chat_id=SUPERGROUP, user_id=42, is_bot=False,
                 sender_chat=None, has_message=True):
        self.effective_chat = FakeChat(chat_id) if chat_id is not None else None
        self.effective_user = FakeUser(user_id, is_bot) if user_id else None
        self.effective_message = FakeMessage(sender_chat) if has_message else None


class FakeContext:
    def __init__(self, bridge):
        self.bot_data = {"bridge": bridge} if bridge else {}


class TestAuthorize:
    def test_allowlisted_user_may_send(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        assert authorize(FakeUpdate(user_id=42), ctx) is True

    def test_other_users_are_refused(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        assert authorize(FakeUpdate(user_id=43), ctx) is False

    def test_an_event_without_a_user_is_refused(self):
        """``if allowed and user and ...`` let these through entirely (audit S03)."""
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        assert authorize(FakeUpdate(user_id=None), ctx) is False

    def test_an_anonymous_admin_is_refused(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        update = FakeUpdate(user_id=42, sender_chat=FakeChat(SUPERGROUP))
        assert authorize(update, ctx) is False

    def test_a_bot_is_refused(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        assert authorize(FakeUpdate(user_id=42, is_bot=True), ctx) is False

    def test_another_chat_is_refused(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        assert authorize(FakeUpdate(chat_id=-999, user_id=42), ctx) is False

    def test_missing_chat_is_refused(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({42})))
        assert authorize(FakeUpdate(chat_id=None, user_id=42), ctx) is False

    def test_an_unconfigured_bridge_refuses_everything(self):
        assert authorize(FakeUpdate(), FakeContext(None)) is False

    def test_open_mode_allows_any_member(self):
        ctx = FakeContext(make_bridge(allow_any=True))
        assert authorize(FakeUpdate(user_id=999), ctx) is True

    def test_open_mode_still_refuses_anonymous_posts(self):
        ctx = FakeContext(make_bridge(allow_any=True))
        update = FakeUpdate(user_id=999, sender_chat=FakeChat(SUPERGROUP))
        assert authorize(update, ctx) is False

    def test_closed_by_default_when_nothing_is_configured(self):
        ctx = FakeContext(make_bridge())
        assert authorize(FakeUpdate(user_id=1), ctx) is False


class TestManagementRights:
    def test_admins_are_separate_from_senders(self):
        bridge = make_bridge(allowed=frozenset({1, 2}), admins=frozenset({1}))
        ctx = FakeContext(bridge)
        assert authorize(FakeUpdate(user_id=2), ctx) is True
        assert authorize(FakeUpdate(user_id=2), ctx, manage=True) is False
        assert authorize(FakeUpdate(user_id=1), ctx, manage=True) is True

    def test_without_admins_senders_may_manage(self):
        ctx = FakeContext(make_bridge(allowed=frozenset({1})))
        assert authorize(FakeUpdate(user_id=1), ctx, manage=True) is True

    def test_zero_is_never_a_valid_user(self):
        bridge = make_bridge(allow_any=True)
        assert bridge.may_send(0) is False
        assert bridge.may_manage(-5) is False


class TestParseMaxInvite:
    def test_a_join_link_is_accepted(self):
        assert parse_max_invite("https://max.ru/join/abc123") == "https://max.ru/join/abc123"

    def test_the_web_host_is_accepted(self):
        assert parse_max_invite("https://web.max.ru/join/abc") is not None

    def test_a_lookalike_domain_is_refused(self):
        """``"max.ru/" in link`` accepted anything containing that substring."""
        assert parse_max_invite("https://evil.example/?x=max.ru/join/abc") is None

    def test_a_subdomain_attack_is_refused(self):
        assert parse_max_invite("https://max.ru.evil.example/join/abc") is None

    def test_a_user_share_link_is_refused_for_now(self):
        assert parse_max_invite("https://max.ru/u/token") is None

    def test_an_empty_join_path_is_refused(self):
        assert parse_max_invite("https://max.ru/join/") is None

    def test_a_non_http_scheme_is_refused(self):
        assert parse_max_invite("javascript:alert(1)") is None

    def test_garbage(self):
        assert parse_max_invite("") is None


class TestParseMaxChatId:
    def test_plain_id(self):
        assert parse_max_chat_id("-75107924425434") == -75107924425434

    def test_web_url(self):
        assert parse_max_chat_id("https://web.max.ru/-42") == -42

    def test_bare_url(self):
        assert parse_max_chat_id("https://max.ru/-42") == -42

    def test_nonsense(self):
        assert parse_max_chat_id("nope") is None


class TestUtf16Offsets:
    def test_ascii_is_unchanged(self):
        assert utf16_to_char_offset("hello", 3) == 3

    def test_emoji_take_two_code_units(self):
        text = "👍ok"
        assert utf16_to_char_offset(text, 2) == 1

    def test_zero_and_negative(self):
        assert utf16_to_char_offset("abc", 0) == 0
        assert utf16_to_char_offset("abc", -5) == 0

    def test_empty_text(self):
        assert utf16_to_char_offset("", 5) == 0


@dataclass
class FakeEntity:
    type: object
    offset: int
    length: int
    url: str | None = None


class TestEntitiesToMaxElements:
    def test_no_entities(self):
        assert entities_to_max_elements("abc", None) == []

    @pytest.mark.parametrize("tg_type,max_type", [
        (MessageEntityType.BOLD, "STRONG"),
        (MessageEntityType.ITALIC, "EMPHASIZED"),
        (MessageEntityType.STRIKETHROUGH, "STRIKETHROUGH"),
        (MessageEntityType.UNDERLINE, "UNDERLINE"),
        (MessageEntityType.CODE, "MONOSPACED"),
        (MessageEntityType.PRE, "MONOSPACED"),
    ])
    def test_supported_styles_map(self, tg_type, max_type):
        elements = entities_to_max_elements("abcdef", [FakeEntity(tg_type, 1, 3)])
        assert elements == [{"type": max_type, "from": 1, "length": 3}]

    def test_text_link_carries_the_url(self):
        elements = entities_to_max_elements(
            "click", [FakeEntity(MessageEntityType.TEXT_LINK, 0, 5,
                                 url="https://x")])
        assert elements[0]["type"] == "LINK"
        assert elements[0]["attributes"] == {"url": "https://x"}

    def test_unsupported_entities_are_dropped_not_sent(self):
        """MAX rejects unknown element names and fails the whole send."""
        assert entities_to_max_elements(
            "abc", [FakeEntity(MessageEntityType.SPOILER, 0, 3)]) == []

    def test_offsets_are_converted_from_utf16(self):
        elements = entities_to_max_elements(
            "👍bold", [FakeEntity(MessageEntityType.BOLD, 2, 4)])
        assert elements == [{"type": "STRONG", "from": 1, "length": 4}]

    def test_zero_length_is_dropped(self):
        assert entities_to_max_elements(
            "abc", [FakeEntity(MessageEntityType.BOLD, 1, 0)]) == []


class TestDescribeMedia:
    def _message(self, **attrs):
        base = dict(photo=None, voice=None, audio=None, animation=None,
                    video=None, video_note=None, sticker=None, document=None,
                    location=None, contact=None)
        base.update(attrs)
        return type("Msg", (), base)()

    def test_photo_picks_the_largest_size(self):
        small = type("P", (), {"file_id": "small", "file_size": 10})()
        large = type("P", (), {"file_id": "large", "file_size": 900})()
        media = _describe_media(self._message(photo=[small, large]))
        assert media["file_id"] == "large"
        assert media["kind"] == "photo"

    def test_document_keeps_its_name_and_type(self):
        doc = type("D", (), {"file_id": "f", "file_size": 5,
                             "file_name": "report.pdf",
                             "mime_type": "application/pdf"})()
        media = _describe_media(self._message(document=doc))
        assert media["filename"] == "report.pdf"
        assert media["mime"] == "application/pdf"

    def test_animated_sticker_degrades_to_its_emoji(self):
        """MAX has no animated sticker attachment; the emoji is the honest fallback."""
        sticker = type("S", (), {"is_animated": True, "is_video": False,
                                 "emoji": "🔥", "file_id": "s",
                                 "file_size": 1})()
        assert _describe_media(self._message(sticker=sticker)) == {
            "kind": "sticker_text", "emoji": "🔥"}

    def test_static_sticker_is_sent_as_a_file(self):
        sticker = type("S", (), {"is_animated": False, "is_video": False,
                                 "emoji": "🙂", "file_id": "s",
                                 "file_size": 100})()
        assert _describe_media(self._message(sticker=sticker))["kind"] == "sticker"

    def test_video_note_is_recognised(self):
        note = type("V", (), {"file_id": "v", "file_size": 10})()
        assert _describe_media(self._message(video_note=note))["kind"] == "video_note"

    def test_location_carries_coordinates(self):
        loc = type("L", (), {"latitude": 0.0, "longitude": 0.0})()
        media = _describe_media(self._message(location=loc))
        assert media == {"kind": "location", "lat": 0.0, "lon": 0.0}

    def test_contact_is_recognised(self):
        person = type("C", (), {"phone_number": "+70000000000",
                                "first_name": "Алиса", "last_name": None})()
        assert _describe_media(self._message(contact=person))["kind"] == "contact"

    def test_plain_text_has_no_media(self):
        assert _describe_media(self._message()) is None


class TestStatusReport:
    """``/status`` must read each number from where it is actually kept.

    The quarantine figure was read off the journal, where nothing ever writes
    that status — a frame we refuse to parse has no message ID to file it
    under. It therefore printed a structural zero while the real count sat
    unreported in the health counters (audit §10.4).
    """

    class _Pipeline:
        def __init__(self):
            self._stats = {"pending": 3, "failed": 1, "uncertain": 2,
                           "quarantined": 0, "oldest_pending_age": 42.0}

        async def stats(self):
            return dict(self._stats)

    class _Reply:
        def __init__(self):
            self.text = ""

        async def reply_text(self, text, **kwargs):
            self.text = text

    async def _render(self, bridge) -> str:
        from app.tg_handler import cmd_status

        reply = self._Reply()
        update = FakeUpdate()
        update.effective_message = reply
        context = FakeContext(bridge)
        await cmd_status(update, context)
        return reply.text

    async def test_quarantine_comes_from_the_wire_counter(self):
        bridge = make_bridge(allowed=frozenset({42}))
        bridge.health.counters.bump("quarantined", 4)
        bridge.max_pipeline = self._Pipeline()
        text = await self._render(bridge)
        assert "в карантине 4" in text

    async def test_edits_are_reported_next_to_replays(self):
        bridge = make_bridge(allowed=frozenset({42}))
        bridge.health.counters.bump("replays", 5)
        bridge.health.counters.bump("edits", 2)
        text = await self._render(bridge)
        assert "повторов 5" in text
        assert "правок 2" in text

    async def test_queue_depth_still_comes_from_the_journal(self):
        bridge = make_bridge(allowed=frozenset({42}))
        bridge.max_pipeline = self._Pipeline()
        text = await self._render(bridge)
        assert "в очереди 3" in text
        assert "провалено 1" in text
        assert "неизвестно 2" in text

    async def test_an_unauthorised_user_gets_nothing(self):
        bridge = make_bridge(allowed=frozenset({999}))
        assert await self._render(bridge) == ""
