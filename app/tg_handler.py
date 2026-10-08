"""Telegram ingress: commands, and replies routed back into MAX.

Two structural changes over the previous version.

**Ingress is fast and durable.** A handler used to download a Telegram video,
upload it to MAX and wait for the server to finish ingesting it — all inside the
update handler, which python-telegram-bot runs one at a time. One slow upload
blocked every command in every other topic, and if the process died in the
middle the update was simply gone (audit S01/S02). Handlers now write the update
into the same journal the other direction uses and return; a per-chat worker does
the slow part and reports back on the original message.

**Authorisation is fail-closed.** ``if allowed_user_id and user.id != allowed``
let an event with no ``effective_user`` through, treated ``0`` as "no
restriction", and never checked that a callback button came from the right
supergroup (audit S03). Every entry point goes through :func:`authorize` now,
and the configuration refuses to start in "anyone in the group may send as me"
mode unless that was chosen explicitly.

The ``/profile`` opcode probing is gone: firing unverified payloads at the live
socket is how you get the connection torn down by a validation error, and a user
command is not the place to reverse-engineer a protocol (audit T08).
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Update,
)
from telegram.constants import MessageEntityType
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.errors import ConnectionLost, MaxRpcError, NotReady, describe_exception
from app.health import Health, format_duration
from app.max_client import MaxClient
from app.pipeline import DeliveryPipeline
from app.resolver import ContactResolver
from app.store import chat_key
from app.tg_sender import escape, is_silent
from app.topics import TITLE_MANUAL, TopicService

log = logging.getLogger(__name__)

BRIDGE_KEY = "bridge"

# Telegram entity type → MAX element type. MAX's WebSocket protocol exposes a
# smaller enum than its HTTP bot API: CODE_BLOCK / HIGHLIGHTED / HEADING are all
# rejected with "No enum constant", and BLOCKQUOTE has no counterpart at all, so
# those degrade to the nearest thing MAX renders rather than failing the send.
_TG_TO_MAX_ELEMENT_TYPE = {
    MessageEntityType.BOLD: "STRONG",
    MessageEntityType.ITALIC: "EMPHASIZED",
    MessageEntityType.STRIKETHROUGH: "STRIKETHROUGH",
    MessageEntityType.UNDERLINE: "UNDERLINE",
    MessageEntityType.CODE: "MONOSPACED",
    MessageEntityType.PRE: "MONOSPACED",
}

_MAX_CHAT_URL_RE = re.compile(r"^https?://(?:web\.)?max\.ru/(-?\d+)$")
_MAX_LINK_RE = re.compile(r"https?://(?:web\.)?max\.ru/\S+")

# The Bot API caps what a bot may *fetch* through getFile at 20 MB. What it may
# send is far larger — conflating the two is the documentation error the audit
# called out, so the two limits are named separately here.
TG_GETFILE_LIMIT = 20 * 1024 * 1024


def parse_max_invite(link: str) -> str | None:
    """Validate a MAX invite link by parsing it, not by substring matching.

    ``"max.ru/" in link`` accepted ``https://evil.example/?x=max.ru/`` (audit
    S05). Only ``max.ru`` and ``web.max.ru`` with a ``/join/`` path are allowed;
    ``/u/<token>`` share links are a documented gap, and the command says so
    instead of failing obscurely.
    """
    try:
        parts = urlsplit(link.strip())
    except ValueError:
        return None
    if parts.scheme not in ("http", "https"):
        return None
    host = (parts.hostname or "").lower()
    if host not in ("max.ru", "web.max.ru"):
        return None
    if not parts.path.startswith("/join/") or len(parts.path) <= len("/join/"):
        return None
    return f"https://max.ru{parts.path}"


def parse_max_chat_id(value: str) -> int | None:
    value = value.strip()
    try:
        return int(value)
    except ValueError:
        pass
    match = _MAX_CHAT_URL_RE.match(value)
    return int(match.group(1)) if match else None


def utf16_to_char_offset(text: str, utf16_offset: int) -> int:
    """Telegram counts entity offsets in UTF-16 code units; MAX uses codepoints."""
    if utf16_offset <= 0 or not text:
        return 0
    encoded = text.encode("utf-16-le")
    return len(encoded[: utf16_offset * 2].decode("utf-16-le", errors="ignore"))


def entities_to_max_elements(text: str, entities) -> list:
    if not entities:
        return []
    elements: list[dict] = []
    for entity in entities:
        start = utf16_to_char_offset(text, entity.offset)
        end = utf16_to_char_offset(text, entity.offset + entity.length)
        length = end - start
        if length <= 0:
            continue
        max_type = _TG_TO_MAX_ELEMENT_TYPE.get(entity.type)
        if max_type:
            elements.append({"type": max_type, "from": start, "length": length})
        elif entity.type == MessageEntityType.TEXT_LINK and getattr(entity, "url", None):
            elements.append({"type": "LINK", "from": start, "length": length,
                             "attributes": {"url": entity.url}})
    return elements


@dataclass
class Bridge:
    """Everything a handler needs, assembled once at startup."""

    client: MaxClient
    resolver: ContactResolver
    topics: TopicService
    health: Health
    supergroup_id: int
    allowed_user_ids: frozenset[int]
    admin_user_ids: frozenset[int]
    allow_any_member: bool
    max_pipeline: DeliveryPipeline | None = None
    tg_pipeline: DeliveryPipeline | None = None

    def may_send(self, user_id: int | None) -> bool:
        if user_id is None or user_id <= 0:
            return False
        if self.allowed_user_ids:
            return user_id in self.allowed_user_ids
        return self.allow_any_member

    def may_manage(self, user_id: int | None) -> bool:
        if user_id is None or user_id <= 0:
            return False
        if self.admin_user_ids:
            return user_id in self.admin_user_ids
        if self.allowed_user_ids:
            return user_id in self.allowed_user_ids
        return self.allow_any_member


def get_bridge(context: ContextTypes.DEFAULT_TYPE) -> Bridge:
    return context.bot_data[BRIDGE_KEY]


def authorize(update: Update, context: ContextTypes.DEFAULT_TYPE, *,
              manage: bool = False) -> bool:
    """Fail-closed gate for every entry point.

    Anonymous admins and channel-sender posts are refused: they carry a
    ``sender_chat`` rather than a user, so there is no identity to check against
    the allowlist.
    """
    bridge = context.bot_data.get(BRIDGE_KEY)
    if bridge is None:
        return False
    chat = update.effective_chat
    if chat is None or chat.id != bridge.supergroup_id:
        return False
    message = update.effective_message
    if message is not None and getattr(message, "sender_chat", None) is not None:
        return False
    user = update.effective_user
    if user is None or user.is_bot:
        return False
    return bridge.may_manage(user.id) if manage else bridge.may_send(user.id)


async def resolve_topic_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The MAX chat bound to the topic this message is in, if any."""
    message = update.effective_message
    if message is None:
        return None
    thread_id = message.message_thread_id
    if thread_id is None or not message.is_topic_message:
        return None
    bridge = get_bridge(context)
    max_chat_id = await bridge.topics.chat_for_thread(thread_id)
    if max_chat_id is None:
        return None
    return message, max_chat_id


# ── ingress into the outbound journal ──────────────────────────────

def _describe_media(message) -> dict | None:
    """Extract the one attachment a Telegram message carries, if any."""
    if message.photo:
        photo = message.photo[-1]
        return {"kind": "photo", "file_id": photo.file_id,
                "file_size": photo.file_size, "filename": "photo.jpg",
                "mime": "image/jpeg"}
    if message.voice:
        return {"kind": "voice", "file_id": message.voice.file_id,
                "file_size": message.voice.file_size, "filename": "voice.ogg",
                "mime": message.voice.mime_type or "audio/ogg"}
    if message.audio:
        return {"kind": "audio", "file_id": message.audio.file_id,
                "file_size": message.audio.file_size,
                "filename": message.audio.file_name or "audio",
                "mime": message.audio.mime_type or "audio/mpeg"}
    if message.animation:
        return {"kind": "animation", "file_id": message.animation.file_id,
                "file_size": message.animation.file_size,
                "filename": message.animation.file_name or "animation.mp4",
                "mime": message.animation.mime_type or "video/mp4"}
    if message.video:
        return {"kind": "video", "file_id": message.video.file_id,
                "file_size": message.video.file_size,
                "filename": message.video.file_name or "video.mp4",
                "mime": message.video.mime_type or "video/mp4"}
    if message.video_note:
        return {"kind": "video_note", "file_id": message.video_note.file_id,
                "file_size": message.video_note.file_size,
                "filename": "video_note.mp4", "mime": "video/mp4"}
    if message.sticker:
        sticker = message.sticker
        if sticker.is_animated or sticker.is_video:
            # MAX has no animated-sticker attachment; send the emoji instead of
            # a file the other side cannot render.
            return {"kind": "sticker_text", "emoji": sticker.emoji or "🙂"}
        return {"kind": "sticker", "file_id": sticker.file_id,
                "file_size": sticker.file_size, "filename": "sticker.webp",
                "mime": "image/webp"}
    if message.document:
        return {"kind": "document", "file_id": message.document.file_id,
                "file_size": message.document.file_size,
                "filename": message.document.file_name or "file",
                "mime": message.document.mime_type or "application/octet-stream"}
    if message.location:
        return {"kind": "location", "lat": message.location.latitude,
                "lon": message.location.longitude}
    if message.contact:
        return {"kind": "contact", "phone": message.contact.phone_number,
                "first_name": message.contact.first_name,
                "last_name": message.contact.last_name}
    return None


async def _enqueue_outbound(update: Update, context: ContextTypes.DEFAULT_TYPE,
                            *, edited: bool = False) -> None:
    """Journal a Telegram message for delivery to MAX, then return immediately."""
    if not authorize(update, context):
        return
    target = await resolve_topic_chat(update, context)
    if not target:
        return
    message, max_chat_id = target
    bridge = get_bridge(context)
    if bridge.tg_pipeline is None:
        return

    media = _describe_media(message)
    text = message.text or message.caption or ""
    entities = message.entities if message.text else message.caption_entities
    if not text and media is None:
        return

    if media and media.get("file_size") and media["file_size"] > TG_GETFILE_LIMIT:
        await message.reply_text(
            f"⚠️ Файл больше {TG_GETFILE_LIMIT // (1024 * 1024)} МБ — "
            "Bot API не даёт мне его скачать, поэтому в MAX он не уйдёт."
        )
        return

    payload = {
        "max_chat_id": max_chat_id,
        "tg_chat_id": message.chat_id,
        "tg_message_id": message.message_id,
        "thread_id": message.message_thread_id,
        "text": text,
        "elements": entities_to_max_elements(text, entities),
        "media": media,
        "media_group_id": message.media_group_id,
        "reply_to": (message.reply_to_message.message_id
                     if message.reply_to_message else None),
        "edited": edited,
    }
    # The revision is what makes an edit a new job rather than a duplicate.
    from app.store import revision_hash
    revision = revision_hash({"text": text, "media": media,
                              "elements": payload["elements"]})
    accepted = await bridge.tg_pipeline.submit(
        chat=max_chat_id, event_id=str(message.message_id),
        revision=revision, payload=payload)
    if not accepted:
        log.debug("Telegram message %s is already journalled", message.message_id)


async def on_topic_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _enqueue_outbound(update, context)


async def on_edited_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Edits used to be ignored entirely — handlers only read ``update.message``."""
    await _enqueue_outbound(update, context, edited=True)


# ── commands ───────────────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not authorize(update, context):
        return
    await update.effective_message.reply_text(
        HELP_TEXT, parse_mode="HTML", disable_web_page_preview=True)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """What the bridge actually thinks its own state is."""
    if not authorize(update, context, manage=True):
        return
    bridge = get_bridge(context)
    snapshot = bridge.health.snapshot()
    counters = snapshot["counters"]

    lines = [
        f"<b>Связь с MAX:</b> {escape(snapshot['state'])} "
        f"({format_duration(snapshot['state_age'])})",
        f"<b>Последний кадр:</b> {format_duration(snapshot['last_frame_age'])} назад",
        f"<b>Воркеров чатов:</b> {snapshot['workers']}",
    ]
    if snapshot.get("incident_age") is not None:
        lines.append(f"<b>Инцидент длится:</b> "
                     f"{format_duration(snapshot['incident_age'])}")
    if snapshot.get("last_outage_sec"):
        lines.append(f"<b>Последний разрыв:</b> "
                     f"{format_duration(snapshot['last_outage_sec'])} "
                     f"<i>(докачки истории нет — см. /help)</i>")
    if snapshot.get("last_error"):
        lines.append(f"<b>Последняя ошибка:</b> "
                     f"<code>{escape(snapshot['last_error'])[:200]}</code>")

    for title, pipeline in (("MAX → TG", bridge.max_pipeline),
                            ("TG → MAX", bridge.tg_pipeline)):
        if pipeline is None:
            continue
        stats = await pipeline.stats()
        lines.append(
            f"\n<b>{title}</b>: в очереди {stats['pending']}, "
            f"провалено {stats['failed']}, неизвестно {stats['uncertain']}")
        if stats["oldest_pending_age"]:
            lines.append(f"Самое старое незавершённое: "
                         f"{format_duration(stats['oldest_pending_age'])}")

    # Quarantine is counted on the wire, not in the journal: a frame we refuse
    # to parse has no message ID to file it under. Reading it off the journal
    # here printed a number that was structurally always zero.
    lines.append(
        f"\n<b>Счётчики:</b> принято {counters['accepted']}, "
        f"доставлено {counters['delivered']}, повторов {counters['replays']}, "
        f"правок {counters['edits']}, в карантине {counters['quarantined']}, "
        f"реконнектов {counters['reconnects']}, "
        f"таймаутов RPC {counters['rpc_timeouts']}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_retry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Re-queue failed and uncertain jobs.

    Uncertain ones are included deliberately and with a warning: Telegram may
    already have accepted them, so forcing a retry can duplicate a message. That
    is a decision for the owner, not something to do automatically (audit D06).
    """
    if not authorize(update, context, manage=True):
        return
    bridge = get_bridge(context)
    message = update.effective_message
    scope = (context.args or ["all"])[0].lower()

    chat_filter = None
    if scope == "here":
        target = await resolve_topic_chat(update, context)
        if not target:
            await message.reply_text("«/retry here» работает только внутри топика.")
            return
        chat_filter = target[1]

    total = 0
    for pipeline in (bridge.max_pipeline, bridge.tg_pipeline):
        if pipeline is not None:
            total += await pipeline.retry_failed(chat=chat_filter)
    await message.reply_text(
        f"Поставлено в очередь заново: <b>{total}</b>.\n"
        "<i>Среди них могут быть сообщения со статусом «результат неизвестен» — "
        "их повтор может создать дубль.</i>" if total else
        "Нечего повторять — провалившихся задач нет.",
        parse_mode="HTML")


async def cmd_bind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not authorize(update, context, manage=True):
        return
    message = update.effective_message
    args = context.args or []
    if not args:
        await message.reply_text(
            "Использование: <code>/bind &lt;chat_id или https://web.max.ru/-...&gt;"
            " [название]</code>", parse_mode="HTML")
        return

    max_chat_id = parse_max_chat_id(args[0])
    if max_chat_id is None:
        await message.reply_text(
            "Не понял chat_id. Пример: <code>/bind -75107924425434</code>",
            parse_mode="HTML")
        return

    bridge = get_bridge(context)
    existing = await bridge.topics.get_thread(max_chat_id)
    if existing is not None:
        await message.reply_text(
            f"Этот чат MAX уже привязан к топику "
            f"(thread_id=<code>{existing}</code>).", parse_mode="HTML")
        return

    if len(args) > 1:
        title, origin = " ".join(args[1:]).strip(), TITLE_MANUAL
    else:
        await bridge.resolver.ensure_chat(max_chat_id)
        known = bridge.resolver.chat_name(max_chat_id)
        title = known if known != str(max_chat_id) else str(max_chat_id)
        origin = None

    resolution = await bridge.topics.bind(max_chat_id, title, origin or TITLE_MANUAL)
    if not resolution.ok:
        await message.reply_text(f"Не удалось создать топик: {resolution.error}")
        return

    await message.reply_text(
        f"Готово: <b>{escape(title)}</b> ↔ MAX <code>{max_chat_id}</code> "
        f"(thread_id=<code>{resolution.thread_id}</code>).", parse_mode="HTML")
    await post_topic_intro(context.bot, bridge.supergroup_id, bridge.client,
                           bridge.resolver, max_chat_id, resolution.thread_id)


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not authorize(update, context, manage=True):
        return
    message = update.effective_message
    args = context.args or []
    raw = args[0] if args else ""
    if not raw and message.text:
        found = _MAX_LINK_RE.search(message.text)
        raw = found.group(0) if found else ""

    link = parse_max_invite(raw)
    if link is None:
        await message.reply_text(
            "Использование: <code>/add https://max.ru/join/...</code>\n"
            "<i>Личные ссылки вида /u/... пока не поддерживаются: opcode 57 ищет "
            "их в пространстве чатов и отвечает not.found.</i>",
            parse_mode="HTML")
        return

    bridge = get_bridge(context)
    try:
        resp = await bridge.client.open_by_link(link)
    except MaxRpcError as exc:
        await message.reply_text(f"⚠️ MAX: {escape(exc.describe())}")
        return
    except (NotReady, ConnectionLost) as exc:
        await message.reply_text(f"⚠️ Связь с MAX недоступна: {escape(str(exc))}")
        return
    except TimeoutError:
        await message.reply_text("⚠️ Таймаут от MAX, ссылка не открылась.")
        return

    chat_id = _extract_chat_id(resp)
    if chat_id is None:
        await message.reply_text(
            "MAX принял ссылку, но не вернул понятный chat_id.")
        return

    if isinstance(resp.get("chat"), dict):
        bridge.resolver.ingest_chat(resp["chat"])

    existing = await bridge.topics.get_thread(chat_id)
    if existing is not None:
        await message.reply_text(
            f"Уже привязан к топику (thread_id=<code>{existing}</code>).",
            parse_mode="HTML")
        return

    title = bridge.resolver.chat_name(chat_id)
    if title == str(chat_id) and bridge.resolver.is_dm(chat_id):
        peer = bridge.resolver.peer_in_dm(chat_id)
        if peer is not None:
            await bridge.resolver.fetch_contact(peer, force_refresh=True)
            title = bridge.resolver.user_name(peer)

    resolution = await bridge.topics.bind(chat_id, title)
    if not resolution.ok:
        await message.reply_text(f"Не удалось создать топик: {resolution.error}")
        return
    await message.reply_text(
        f"Готово: <b>{escape(title)}</b> ↔ MAX <code>{chat_id}</code> "
        f"(thread_id=<code>{resolution.thread_id}</code>).", parse_mode="HTML")
    await post_topic_intro(context.bot, bridge.supergroup_id, bridge.client,
                           bridge.resolver, chat_id, resolution.thread_id)


def _extract_chat_id(resp: Any) -> int | None:
    if not isinstance(resp, dict):
        return None
    for key in ("chatId", "conversationId"):
        value = resp.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.lstrip("-").isdigit():
            return int(value)
    chat = resp.get("chat")
    if isinstance(chat, dict):
        cid = chat.get("id")
        if isinstance(cid, int):
            return cid
        if isinstance(cid, str) and cid.lstrip("-").isdigit():
            return int(cid)
    return None


async def cmd_del(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not authorize(update, context, manage=True):
        return
    message = update.effective_message
    target = await resolve_topic_chat(update, context)
    if not target:
        await message.reply_text(
            "Команда работает только внутри топика, связанного с MAX-чатом.")
        return
    _msg, max_chat_id = target
    bridge = get_bridge(context)
    record = await bridge.topics.get_record(max_chat_id)
    if record is None:
        await message.reply_text("Связь уже снята.")
        return

    # The revision is carried in the button so a stale one pressed after a
    # re-bind deletes nothing (audit T05).
    data = f"del:ok:{record.thread_id}:{record.revision}:{chat_key(max_chat_id)}"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🗑 Удалить топик", callback_data=data),
        InlineKeyboardButton("Отмена", callback_data="del:cancel"),
    ]])
    await message.reply_text(
        f"Удалить этот топик и снять связь с MAX-чатом "
        f"<code>{escape(max_chat_id)}</code>?\n\nВосстановить нельзя.",
        parse_mode="HTML", reply_markup=keyboard)


async def on_del_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or not query.data:
        return
    if not authorize(update, context, manage=True):
        await query.answer("Недостаточно прав.", show_alert=True)
        return
    await query.answer()

    parts = query.data.split(":")
    if parts[:2] == ["del", "cancel"]:
        try:
            await query.edit_message_text("Отменено.")
        except Exception:
            log.debug("Could not edit the cancel message", exc_info=True)
        return
    if len(parts) != 5 or parts[0] != "del" or parts[1] != "ok":
        return
    try:
        thread_id = int(parts[2])
        revision = int(parts[3])
    except ValueError:
        return
    chat_token = parts[4]
    max_chat_id: Any = int(chat_token) if chat_token.lstrip("-").isdigit() else chat_token

    bridge = get_bridge(context)
    ok, error = await bridge.topics.delete_topic(max_chat_id, thread_id, revision)
    if ok:
        log.info("Deleted topic thread=%s for MAX chat %s", thread_id,
                 chat_key(max_chat_id))
        return
    try:
        await query.edit_message_text(f"⚠️ Не удалено: {escape(error or '')}")
    except Exception:
        log.debug("Could not report the delete failure", exc_info=True)


async def cmd_intro(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not authorize(update, context, manage=True):
        return
    message = update.effective_message
    target = await resolve_topic_chat(update, context)
    if not target:
        await message.reply_text(
            "Команда работает только внутри топика, связанного с чатом MAX.")
        return
    _msg, max_chat_id = target
    bridge = get_bridge(context)
    await post_topic_intro(context.bot, bridge.supergroup_id, bridge.client,
                           bridge.resolver, max_chat_id,
                           message.message_thread_id, force_refresh=True)


async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not authorize(update, context):
        return
    message = update.effective_message
    target = await resolve_topic_chat(update, context)
    if not target:
        await message.reply_text(
            "Команда работает только внутри топика, связанного с чатом MAX.")
        return
    _msg, max_chat_id = target
    bridge = get_bridge(context)
    resolver = bridge.resolver

    await resolver.ensure_chat(max_chat_id)
    if not resolver.is_dm(max_chat_id):
        await message.reply_text(_chat_card(resolver, max_chat_id),
                                 parse_mode="HTML")
        return

    peer_id = resolver.peer_in_dm(max_chat_id)
    if peer_id is None:
        await message.reply_text("Не нашёл собеседника в этом чате.")
        return

    contact = await resolver.fetch_contact(peer_id, force_refresh=True)
    if contact is None:
        name = resolver.user_name(peer_id)
        await message.reply_text(
            f"<b>{escape(name)}</b>\nid: <code>{peer_id}</code>\n\n"
            "<i>MAX не отдаёт расширенный профиль для этого собеседника — "
            "скорее всего, он не у тебя в контактах. Телефон и «о себе» "
            "CONTACT_GET не возвращает в принципе.</i>",
            parse_mode="HTML")
        return

    body, photo_url = _contact_card(resolver, peer_id, contact)
    if photo_url:
        data = await bridge.client.download_small(photo_url)
        if data:
            try:
                await context.bot.send_photo(
                    chat_id=message.chat_id,
                    photo=InputFile(io.BytesIO(data), filename="profile.jpg"),
                    caption=body, parse_mode="HTML",
                    message_thread_id=message.message_thread_id)
                return
            except Exception:
                log.info("Could not send the profile photo", exc_info=True)
    await message.reply_text(body, parse_mode="HTML")


def _contact_card(resolver: ContactResolver, peer_id: Any,
                  contact: dict) -> tuple[str, str | None]:
    name = resolver.user_name(peer_id)
    lines = [f"<b>{escape(name)}</b>", f"id: <code>{escape(peer_id)}</code>"]
    phone = contact.get("phone")
    if phone:
        lines.append(f"📞 <code>{escape(phone)}</code>")
    username = contact.get("link") or contact.get("username")
    if username:
        lines.append(f"🔗 {escape(username)}")
    about = (contact.get("description") or contact.get("about")
             or contact.get("status"))
    if about:
        lines.append(f"\n{escape(about)}")

    photo_url = None
    nested = contact.get("photo") or contact.get("avatar")
    if isinstance(nested, dict):
        photo_url = (nested.get("baseUrl") or nested.get("url")
                     or nested.get("photoUrl"))
    photo_url = (photo_url or contact.get("baseUrl") or contact.get("baseRawUrl")
                 or contact.get("photoUrl") or contact.get("baseRawIconUrl"))
    return "\n".join(lines), photo_url


def _chat_card(resolver: ContactResolver, chat_id: Any) -> str:
    chat = resolver.chat_raw(chat_id)
    title = chat.get("title") or resolver.chat_name(chat_id)
    ctype = chat.get("type") or "?"
    participants = chat.get("participants") or {}
    lines = [f"<b>{escape(title)}</b> · {escape(ctype)}",
             f"id: <code>{escape(chat_id)}</code>",
             f"Участников: <b>{len(participants)}</b>"]
    if chat.get("description"):
        lines.append(f"\n{escape(chat['description'])}")
    if chat.get("link"):
        lines.append(f"\n🔗 {escape(chat['link'])}")
    return "\n".join(lines)


async def post_topic_intro(bot, supergroup_id: int, client: MaxClient,
                           resolver: ContactResolver, max_chat_id: Any,
                           thread_id: int, *, pin: bool = True,
                           force_refresh: bool = False) -> None:
    """Publish (and pin) the profile card that opens a topic."""
    await resolver.ensure_chat(max_chat_id)
    if resolver.is_dm(max_chat_id):
        peer_id = resolver.peer_in_dm(max_chat_id)
        if peer_id is None:
            return
        contact = await resolver.fetch_contact(peer_id, force_refresh=force_refresh)
        body, photo_url = _contact_card(resolver, peer_id, contact or {})
    else:
        body = _chat_card(resolver, max_chat_id)
        chat = resolver.chat_raw(max_chat_id)
        photo_url = chat.get("baseRawIconUrl") or chat.get("baseUrl")

    sent = None
    if photo_url:
        data = await client.download_small(photo_url)
        if data:
            try:
                sent = await bot.send_photo(
                    chat_id=supergroup_id,
                    photo=InputFile(io.BytesIO(data), filename="profile.jpg"),
                    caption=body, parse_mode="HTML",
                    message_thread_id=thread_id,
                    disable_notification=is_silent())
            except Exception:
                log.info("Intro photo failed; falling back to text", exc_info=True)
    if sent is None:
        try:
            sent = await bot.send_message(chat_id=supergroup_id, text=body,
                                          parse_mode="HTML",
                                          message_thread_id=thread_id,
                                          disable_notification=is_silent())
        except Exception:
            log.info("Could not post the intro card", exc_info=True)
            return
    if pin:
        try:
            await bot.pin_chat_message(chat_id=supergroup_id,
                                       message_id=sent.message_id,
                                       disable_notification=True)
        except Exception:
            log.debug("Could not pin the intro card", exc_info=True)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch-all so a handler exception is logged as ours, not swallowed by PTB."""
    log.error("Unhandled error in a Telegram handler: %s",
              describe_exception(context.error) if context.error else "unknown",
              exc_info=context.error)


HELP_TEXT = (
    "<b>max2tg — мост MAX ↔ Telegram</b>\n\n"
    "Команды в супергруппе:\n"
    "• <code>/bind &lt;chat_id или URL&gt; [название]</code> — привязать топик "
    "к чату MAX.\n"
    "• <code>/add &lt;https://max.ru/join/...&gt;</code> — открыть групповую "
    "ссылку MAX и создать топик.\n"
    "• <code>/profile</code> — профиль собеседника (в топике).\n"
    "• <code>/intro</code> — перепостить карточку профиля, обновив кеш.\n"
    "• <code>/del</code> — удалить топик и снять связь (с подтверждением).\n"
    "• <code>/status</code> — состояние связи, очередей и счётчиков.\n"
    "• <code>/retry [here]</code> — повторить провалившиеся отправки.\n"
    "• <code>/help</code> — эта справка.\n\n"
    "<b>Что переносится в MAX:</b> текст с жирным/курсивом/зачёркнутым/"
    "подчёркнутым и моноширинным, ссылки, фото, видео, документы, аудио, "
    "статичные стикеры, геопозиция и контакты.\n"
    "<b>Ограничения:</b> цитаты и блоки кода MAX не умеет — они приезжают "
    "моноширинными или обычным текстом; голосовые уходят <code>.ogg</code>-"
    "файлом, а не голосовым сообщением; анимированные стикеры заменяются эмодзи; "
    "файлы больше 20 МБ Bot API не отдаёт боту на скачивание.\n\n"
    "Новый собеседник в MAX — топик создастся сам, с карточкой в начале."
)


def build_tg_app(token: str, bridge: Bridge, *,
                 proxy_url: str | None = None) -> Application:
    """Assemble the Telegram Application with fail-closed filters."""
    builder = Application.builder().token(token)
    if proxy_url:
        builder = builder.proxy(proxy_url).get_updates_proxy(proxy_url)
    app = builder.build()
    app.bot_data[BRIDGE_KEY] = bridge

    chat_filter = filters.Chat(chat_id=bridge.supergroup_id)
    app.add_handler(CommandHandler("bind", cmd_bind, filters=chat_filter))
    app.add_handler(CommandHandler("add", cmd_add, filters=chat_filter))
    app.add_handler(CommandHandler("profile", cmd_profile, filters=chat_filter))
    app.add_handler(CommandHandler("intro", cmd_intro, filters=chat_filter))
    app.add_handler(CommandHandler("del", cmd_del, filters=chat_filter))
    app.add_handler(CommandHandler("status", cmd_status, filters=chat_filter))
    app.add_handler(CommandHandler("retry", cmd_retry, filters=chat_filter))
    app.add_handler(CommandHandler("help", cmd_help, filters=chat_filter))
    app.add_handler(CallbackQueryHandler(on_del_callback, pattern=r"^del:"))

    content = (
        filters.TEXT | filters.PHOTO | filters.VOICE | filters.AUDIO
        | filters.Document.ALL | filters.VIDEO | filters.VIDEO_NOTE
        | filters.ANIMATION | filters.Sticker.ALL | filters.LOCATION
        | filters.CONTACT
    )
    app.add_handler(MessageHandler(
        content & ~filters.COMMAND & chat_filter & filters.UpdateType.MESSAGE,
        on_topic_message))
    app.add_handler(MessageHandler(
        content & ~filters.COMMAND & chat_filter & filters.UpdateType.EDITED_MESSAGE,
        on_edited_message))
    app.add_error_handler(on_error)
    return app
