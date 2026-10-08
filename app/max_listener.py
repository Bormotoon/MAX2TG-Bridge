"""MAX → Telegram: turning a journalled job into posts in a forum topic.

This module is the :class:`~app.pipeline.JobHandler` the scheduler drives, which
is what makes the ordering guarantees real: albums are assembled *before* any
download starts, and the same worker that decided on the group is the one that
sends it. The old design buffered media in a background timer task, so a slow
album could be overtaken by the plain text that came after it (audit M01/M04).

Other corrections worth pointing out while reading:

* ``elements`` from MAX are rendered as Telegram HTML instead of being dropped
  (audit M11) — the README promised this and the code never did it.
* A reply or forward no longer swallows the new message's own attachments
  (audit M05), and quoting uses Telegram's native reply when we know where the
  quoted message landed, rather than re-uploading its media every time.
* Text is only marked as delivered when a send actually confirms it (audit M06).
* Coordinates of exactly 0 are valid and no longer discarded (audit M10).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from app.errors import (
    FileTooLarge,
    Outcome,
    SendResult,
    describe_exception,
    merge_outcomes,
)
from app.health import Health
from app.logging_utils import policy as log_policy
from app.logging_utils import safe_url
from app.max_client import MaxClient, MaxMessage
from app.net import sniff_media_kind
from app.pipeline import DeliveryPipeline
from app.resolver import ContactResolver
from app.store import (
    PART_CONFIRMED,
    PART_FAILED,
    PART_SKIPPED,
    PART_UNCERTAIN,
    Job,
    chat_key,
)
from app.tg_sender import (
    MediaItem,
    TelegramSender,
    escape,
    partial_failure,
    release_all,
    silent,
)
from app.topics import TITLE_PLACEHOLDER, TITLE_RESOLVED, TopicService

log = logging.getLogger(__name__)

SOURCE = "max"

# Attachments that carry no user content and must not produce a post.
IGNORED_ATTACH_TYPES = {"CONTROL", "WIDGET", "INLINE_KEYBOARD", None}

# MAX splits one album across several DISPATCH frames. Frames further apart than
# this are separate messages, not one album.
ALBUM_WINDOW_MS = 15_000
ALBUM_MAX_ITEMS = 30

GROUPABLE_KINDS = {"photo", "video", "animation"}

# MAX element type → Telegram HTML tag.
_ELEMENT_TAGS = {
    "STRONG": "b",
    "BOLD": "b",
    "EMPHASIZED": "i",
    "ITALIC": "i",
    "STRIKETHROUGH": "s",
    "UNDERLINE": "u",
    "MONOSPACED": "code",
    "CODE": "code",
    "CODE_BLOCK": "pre",
    "BLOCKQUOTE": "blockquote",
    "QUOTE": "blockquote",
    "HEADING": "b",
}


def render_max_text(text: str, elements: list | None) -> str:
    """Render MAX inline formatting as Telegram HTML.

    Offsets from MAX are codepoint indices. Overlapping ranges are clamped to
    their enclosing element so the output is always well-formed — Telegram
    rejects the whole message otherwise, and a rejected message is a lost one.
    """
    if not text:
        return ""
    if not elements:
        return escape(text)

    opens: dict[int, list[tuple[int, str, str | None]]] = {}
    for element in elements:
        if not isinstance(element, dict):
            continue
        try:
            start = int(element.get("from", 0))
            length = int(element.get("length", 0))
        except (TypeError, ValueError):
            continue
        if length <= 0 or start < 0 or start >= len(text):
            continue
        end = min(start + length, len(text))
        etype = str(element.get("type") or "").upper()
        if etype == "LINK":
            url = (element.get("attributes") or {}).get("url")
            if not isinstance(url, str) or not url:
                continue
            opens.setdefault(start, []).append((end, "a", url))
        elif etype in _ELEMENT_TAGS:
            opens.setdefault(start, []).append((end, _ELEMENT_TAGS[etype], None))

    out: list[str] = []
    stack: list[tuple[int, str]] = []
    for i in range(len(text) + 1):
        while stack and stack[-1][0] <= i:
            _end, name = stack.pop()
            out.append(f"</{name}>")
        for end, name, url in sorted(opens.get(i, []), key=lambda e: -e[0]):
            if stack and end > stack[-1][0]:
                end = stack[-1][0]
            if end <= i:
                continue
            if name == "a":
                out.append(f'<a href="{escape(url)}">')
            else:
                out.append(f"<{name}>")
            stack.append((end, name))
        if i < len(text):
            out.append(escape(text[i]))
    return "".join(out)


def human_size(n: float) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} ТБ"


def meaningful_attaches(attaches: list | None) -> list[dict]:
    return [a for a in (attaches or [])
            if isinstance(a, dict) and a.get("_type") not in IGNORED_ATTACH_TYPES]


def photo_url(attach: dict) -> str | None:
    """Best available URL for a PHOTO, including the raw variant.

    ``baseRawUrl`` was never tried, so a photo that only carried the raw URL was
    posted as "could not download" (audit M09).
    """
    for key in ("baseUrl", "baseRawUrl", "url"):
        value = attach.get(key)
        if isinstance(value, str) and value.startswith("http"):
            return value
    return None


def file_url(attach: dict) -> str | None:
    value = attach.get("url")
    return value if isinstance(value, str) and value.startswith("http") else None


def coordinate(*values: Any) -> float | None:
    """First usable coordinate. ``0.0`` is a real place, not a missing value."""
    for value in values:
        if value is None or isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        return number
    return None


@dataclass
class _Prepared:
    """Media pulled down and ready to post, plus whatever replaced it."""

    items: list[MediaItem] = field(default_factory=list)
    notice: str = ""
    groupable: bool = False
    outcome: Outcome = Outcome.DELIVERED

    def release(self) -> None:
        release_all(self.items)


class MaxToTelegramHandler:
    """Delivers MAX messages into their Telegram topics, in order."""

    # How long a delivery waits for the MAX link before rendering without it.
    READY_WAIT_SEC = 30.0

    def __init__(self, client: MaxClient, sender: TelegramSender,
                 topics: TopicService, resolver: ContactResolver,
                 health: Health, *, max_media_bytes: int = 48 * 1024 * 1024):
        self.client = client
        self.sender = sender
        self.topics = topics
        self.resolver = resolver
        self.health = health
        self.max_media_bytes = max_media_bytes
        self.pipeline: DeliveryPipeline | None = None
        self._intro_done: set[str] = set()

    def bind(self, pipeline: DeliveryPipeline) -> None:
        self.pipeline = pipeline

    # ── grouping ───────────────────────────────────────────────────

    async def group(self, job: Job, lookahead: list[Job]) -> list[Job]:
        """Merge a run of bare media from one sender into a single album.

        Deliberately conservative: only *consecutive* jobs, only from the same
        sender, only within a time window, and only when the follow-ups carry no
        text of their own. A caption belongs to exactly one message, and merging
        two captioned messages would have to drop one of them — which is how two
        photos with a caption ended up as two separate posts before (audit M02).
        """
        lead = MaxMessage.from_payload(job.payload)
        if not self._is_album_candidate(lead):
            return [job]

        batch = [job]
        total = len(meaningful_attaches(lead.attaches))
        for candidate in lookahead:
            if candidate.id != batch[-1].id + 1 and len(batch) > 1:
                pass  # ids need not be contiguous; ordering is what matters
            msg = MaxMessage.from_payload(candidate.payload)
            if msg.text.strip():
                break
            if chat_key(msg.sender_id) != chat_key(lead.sender_id):
                break
            if not self._is_album_candidate(msg):
                break
            if not self._within_window(lead, msg):
                break
            count = len(meaningful_attaches(msg.attaches))
            if total + count > ALBUM_MAX_ITEMS:
                break
            batch.append(candidate)
            total += count
        if len(batch) > 1:
            log.info("Grouping %d MAX message(s) into one album for chat %s",
                     len(batch), job.chat_key)
        return batch

    @staticmethod
    def _is_album_candidate(msg: MaxMessage) -> bool:
        if msg.link:
            return False
        attaches = meaningful_attaches(msg.attaches)
        if not attaches:
            return False
        return all(a.get("_type") in ("PHOTO", "FILE", "VIDEO") for a in attaches)

    @staticmethod
    def _within_window(first: MaxMessage, other: MaxMessage) -> bool:
        try:
            return abs(float(other.timestamp) - float(first.timestamp)) <= ALBUM_WINDOW_MS
        except (TypeError, ValueError):
            # No usable timestamps — fall back to "do not group", which costs a
            # split album at worst and never merges unrelated messages.
            return False

    # ── delivery ───────────────────────────────────────────────────

    async def process(self, jobs: list[Job]) -> dict[int, Outcome]:
        # History replayed after an outage is journalled with ``silent`` so it
        # lands without pinging the group. A batch that mixes in a live message
        # keeps its notification.
        await self._wait_for_max()
        with silent(all(job.payload.get("silent") for job in jobs)):
            return await self._process(jobs)

    async def _wait_for_max(self) -> None:
        """Give the MAX link a moment before rendering.

        On start-up the journal is replayed before authorisation finishes, and
        the first batch went out with raw numeric IDs in place of the sender
        and chat names. Names need MAX; the delivery itself does not, so after
        the grace period we post anyway rather than burn retry attempts and
        dead-letter history during a long MAX outage.
        """
        if self.client is None or self.health.ready:
            return
        try:
            await asyncio.wait_for(self.health.ready_event.wait(),
                                   timeout=self.READY_WAIT_SEC)
        except TimeoutError:
            log.info("MAX is not ready after %.0fs — delivering without "
                     "fresh names", self.READY_WAIT_SEC)

    async def _process(self, jobs: list[Job]) -> dict[int, Outcome]:
        messages = [(job, MaxMessage.from_payload(job.payload)) for job in jobs]
        chat_id = messages[0][1].chat_id

        # Echo guard. Own messages are journalled and then dropped here, so the
        # decision is visible in the log instead of being invisible upstream.
        if all(msg.is_self for _job, msg in messages):
            log.debug("Skipping %d own message(s) in chat %s",
                      len(messages), chat_key(chat_id))
            return {job.id: Outcome.DELIVERED for job in jobs}

        resolution = await self._resolve_topic(chat_id, messages[0][1])
        if not resolution.ok:
            # No destination means no delivery. Falling back to General would
            # scatter a private conversation into the shared topic and break
            # reply routing (audit T06).
            log.warning("No topic for MAX chat %s (%s) — retrying later",
                        chat_key(chat_id), resolution.error)
            return {job.id: Outcome.TRANSIENT for job in jobs}
        thread_id = resolution.thread_id

        if resolution.created:
            await self._post_intro(chat_id, thread_id)

        if len(messages) > 1:
            return await self._deliver_album(messages, thread_id)
        job, msg = messages[0]
        if await self._is_single_message_album(job, msg):
            return await self._deliver_album(messages, thread_id)
        return {job.id: await self._deliver_one(job, msg, thread_id)}

    async def _is_single_message_album(self, job: Job, msg: MaxMessage) -> bool:
        """One MAX message with several photos is one post, not five.

        Only batches of separate messages used to reach the album path, so a
        five-photo message became five posts, each with its own header. The
        album path cannot carry a quote, a forward or an edit marker, and a
        retry must continue the per-attachment parts it already started; those
        stay with ``_deliver_one``.
        """
        if len(meaningful_attaches(msg.attaches)) < 2 or msg.link:
            return False
        parts = await self._parts(job.id)
        if any(part.status in (PART_CONFIRMED, PART_UNCERTAIN)
               for part in parts.values()):
            # Something from an earlier attempt may already be in the topic;
            # only the per-attachment path knows how to continue from there.
            # A failed album part left nothing behind and does not count.
            return False
        return await self._previous_delivery(job, msg) is None

    async def _resolve_topic(self, chat_id: Any, msg: MaxMessage):
        await self.resolver.ensure_chat(chat_id)
        title, origin = await self._topic_title(chat_id, msg)
        return await self.topics.ensure_topic(chat_id, title, origin)

    async def _topic_title(self, chat_id: Any, msg: MaxMessage) -> tuple[str, str]:
        """Pick a topic name and say how confident we are in it.

        A name derived from whoever wrote first is a placeholder, not a title;
        labelling it as such is what lets a real chat title replace it later
        (audit T07).
        """
        if self.resolver.is_dm(chat_id):
            name = await self.resolver.resolve_user(msg.sender_id)
            if name and name != str(msg.sender_id):
                return name, TITLE_RESOLVED
            return str(chat_id), TITLE_PLACEHOLDER

        chat_title = self.resolver.chat_name(chat_id)
        if chat_title and chat_title != str(chat_id) and not chat_title.startswith("DM:"):
            return chat_title, TITLE_RESOLVED

        name = await self.resolver.resolve_user(msg.sender_id)
        if name and name != str(msg.sender_id):
            return name, TITLE_PLACEHOLDER
        return str(chat_id), TITLE_PLACEHOLDER

    # ── single message ─────────────────────────────────────────────

    async def _deliver_one(self, job: Job, msg: MaxMessage,
                           thread_id: int) -> Outcome:
        parts = await self._parts(job.id)
        header = await self._header(msg)
        body = render_max_text(msg.text, msg.elements)
        reply_to = await self._reply_target(msg)

        # A new revision of a message we already posted is an edit. Telegram
        # cannot edit media in place reliably, so it is shown as a marked
        # follow-up attached to the original rather than as a fresh, unexplained
        # duplicate of the same message (audit D05).
        edited_from = await self._previous_delivery(job, msg)
        if edited_from is not None:
            header = f"{header} · ✏️ <i>изменено</i>"
            reply_to = reply_to or edited_from
            if not parts:
                # Counted on the first attempt only, so a job that is retried
                # does not inflate the figure /status reports next to replays.
                self.health.counters.bump("edits")
        outcomes: list[Outcome] = []
        text_delivered = False

        quote = await self._quote_block(msg)
        forwarded = self._forwarded_content(msg)

        # A forward carries the original's attachments inside the link object;
        # the outer message's own attachments are delivered too, which the old
        # code returned early and skipped entirely (audit M05).
        attach_groups: list[tuple[str, list[dict], str]] = []
        if forwarded is not None:
            attach_groups.append(("fwd", meaningful_attaches(forwarded.attaches),
                                  render_max_text(forwarded.text,
                                                  forwarded.elements)))
        attach_groups.append(("own", meaningful_attaches(msg.attaches), body))

        caption_prefix = header + (f"\n{quote}" if quote else "")
        first_send = True

        for group_name, attaches, group_text in attach_groups:
            if not attaches and not group_text:
                continue
            caption = caption_prefix if first_send else header
            if group_text:
                caption = f"{caption}\n{group_text}"

            if not attaches:
                key = f"text:{group_name}"
                if parts.get(key) and parts[key].status == PART_CONFIRMED:
                    outcomes.append(Outcome.DELIVERED)
                    first_send = False
                    continue
                result = await self.sender.send_text(
                    caption, thread_id, reply_to=reply_to if first_send else None)
                await self._record(job, key, "text", result)
                outcomes.append(result.outcome)
                text_delivered = text_delivered or result.ok
                first_send = False
                continue

            for index, attach in enumerate(attaches):
                key = f"{group_name}:attach:{index}"
                existing = parts.get(key)
                if existing and existing.status == PART_CONFIRMED:
                    outcomes.append(Outcome.DELIVERED)
                    first_send = False
                    continue
                attach_caption = caption if first_send else header
                outcome = await self._deliver_attach(
                    job, key, attach, msg, attach_caption, thread_id,
                    reply_to if first_send else None)
                outcomes.append(outcome)
                first_send = False

            if group_text and not any(o is Outcome.DELIVERED for o in outcomes[-len(attaches):]):
                # The caption rode on an attachment that failed, so the text was
                # not delivered either — send it on its own rather than assume.
                key = f"text-fallback:{group_name}"
                if not (parts.get(key) and parts[key].status == PART_CONFIRMED):
                    result = await self.sender.send_text(
                        f"{header}\n{group_text}", thread_id)
                    await self._record(job, key, "text", result)
                    outcomes.append(result.outcome)

        if not outcomes:
            # No attachments, no text: still say something, so a message never
            # silently disappears from the topic.
            key = "empty"
            if parts.get(key) and parts[key].status == PART_CONFIRMED:
                return Outcome.DELIVERED
            result = await self.sender.send_text(
                f"{caption_prefix}\n<i>[пустое сообщение]</i>", thread_id,
                reply_to=reply_to)
            await self._record(job, key, "text", result)
            return result.outcome

        return merge_outcomes(outcomes)

    # ── album batch ────────────────────────────────────────────────

    async def _deliver_album(self, messages: list[tuple[Job, MaxMessage]],
                             thread_id: int) -> dict[int, Outcome]:
        lead_job, lead_msg = messages[0]
        header = await self._header(lead_msg)
        body = render_max_text(lead_msg.text, lead_msg.elements)
        caption = f"{header}\n{body}" if body else header

        pending: list[tuple[Job, MaxMessage]] = []
        results: dict[int, Outcome] = {}
        for job, msg in messages:
            parts = await self._parts(job.id)
            if parts.get("album") and parts["album"].status == PART_CONFIRMED:
                # Already delivered on a previous attempt: do not re-post it as
                # part of the retried album.
                results[job.id] = Outcome.DELIVERED
            else:
                pending.append((job, msg))

        if not pending:
            return results

        items: list[MediaItem] = []
        prepared: list[_Prepared] = []
        try:
            album_safe = True
            for _job, msg in pending:
                for attach in meaningful_attaches(msg.attaches):
                    ready = await self._prepare(attach, msg)
                    prepared.append(ready)
                    if ready.groupable:
                        items.extend(ready.items)
                    else:
                        # Anything an album cannot carry — a document, a sticker,
                        # a video that only yielded a preview, a download that
                        # failed — disqualifies the whole batch. Folding it in
                        # used to drop the attachment outright while still
                        # marking its job delivered, and turning a failed
                        # download into a note left the job un-retried
                        # (audit M03/M06/M09).
                        album_safe = False

            if not album_safe or len(items) < 2:
                # Per-message delivery handles every one of those cases already,
                # keeps the journal order, and records a part per attachment. It
                # re-prepares the media, which costs a second download on this
                # uncommon path — cheaper than losing the message.
                log.info("Batch of %d message(s) in chat %s is not a clean album "
                         "— delivering each message on its own",
                         len(pending), lead_job.chat_key)
                for job, msg in pending:
                    results[job.id] = await self._deliver_one(job, msg, thread_id)
                return results

            result = await self.sender.send_album(items, thread_id, caption=caption)
            for job, _msg in pending:
                await self._record(job, "album", "album", result)
                results[job.id] = result.outcome
        finally:
            for ready in prepared:
                ready.release()

        return results

    # ── attachments ────────────────────────────────────────────────

    async def _deliver_attach(self, job: Job, key: str, attach: dict,
                              msg: MaxMessage, caption: str, thread_id: int,
                              reply_to: int | None) -> Outcome:
        atype = str(attach.get("_type") or "")

        if atype == "LOCATION":
            lat = coordinate(attach.get("lat"), attach.get("latitude"))
            lon = coordinate(attach.get("lon"), attach.get("lng"),
                             attach.get("longitude"))
            if lat is None or lon is None or not (-90 <= lat <= 90) \
                    or not (-180 <= lon <= 180):
                result = await self.sender.send_text(
                    f"{caption}\n<i>[геолокация без координат]</i>", thread_id)
            else:
                result = await self.sender.send_location(lat, lon, thread_id)
                if result.ok and caption:
                    tail = await self.sender.send_text(caption, thread_id)
                    if tail.ok:
                        result.message_ids.extend(tail.message_ids)
                    else:
                        # The pin landed and its caption did not. Reporting
                        # "delivered" here is exactly how a message body
                        # vanishes without trace (audit M06).
                        result = partial_failure(tail, result.message_ids)
            await self._record(job, key, "location", result)
            return result.outcome

        if atype == "CONTACT":
            phone = str(attach.get("phone") or "")
            name = str(attach.get("name") or "")
            if phone:
                result = await self.sender.send_contact(phone, name, thread_id=thread_id)
            else:
                result = await self.sender.send_text(
                    f"{caption}\n👤 {escape(name)}", thread_id)
            await self._record(job, key, "contact", result)
            return result.outcome

        if atype == "SHARE":
            lines = [caption]
            title = attach.get("title")
            url = attach.get("url")
            desc = attach.get("description")
            if title:
                lines.append(f"🔗 <b>{escape(title)}</b>")
            if isinstance(url, str) and url:
                lines.append(escape(url))
            if desc:
                lines.append(f"<i>{escape(str(desc)[:400])}</i>")
            result = await self.sender.send_text("\n".join(lines), thread_id)
            await self._record(job, key, "share", result)
            return result.outcome

        ready = await self._prepare(attach, msg)
        try:
            if not ready.items:
                text = f"{caption}\n{ready.notice}" if ready.notice else caption
                result = await self.sender.send_text(text, thread_id,
                                                     reply_to=reply_to)
                await self._record(job, key, "placeholder", result)
                # A placeholder is not the attachment. If the download itself was
                # a transient failure, say so, so the job is retried instead of
                # being written off as delivered (audit M09).
                if ready.outcome is Outcome.TRANSIENT and result.ok:
                    return Outcome.TRANSIENT
                return result.outcome

            item = ready.items[0]
            if item.kind == "sticker":
                # Stickers carry no caption: the text needs its own message or it
                # is silently lost (audit M06).
                result = await self.sender.send_media(item, thread_id)
                if result.ok and caption:
                    tail = await self.sender.send_text(caption, thread_id)
                    if tail.ok:
                        result.message_ids.extend(tail.message_ids)
                    else:
                        # The sticker is posted; a retry would post it again.
                        result = partial_failure(tail, result.message_ids)
            else:
                result = await self.sender.send_media(
                    item, thread_id, caption=caption, reply_to=reply_to)
            await self._record(job, key, item.kind, result)
            return result.outcome
        finally:
            ready.release()

    async def _prepare(self, attach: dict, msg: MaxMessage) -> _Prepared:
        """Resolve one attachment into ready-to-send media, or a reason it is not."""
        atype = str(attach.get("_type") or "")
        try:
            if atype == "PHOTO":
                url = photo_url(attach)
                if not url:
                    return _Prepared(notice="<i>[фото без ссылки]</i>",
                                     outcome=Outcome.PERMANENT)
                return await self._download_item(url, "photo.jpg", "photo")

            if atype == "VIDEO":
                url = await self.client.resolve_video_url(
                    attach.get("videoId") or attach.get("id"),
                    msg.chat_id, msg.message_id)
                if url:
                    return await self._download_item(url, "video.mp4", "video")
                thumb = attach.get("thumbnail")
                if isinstance(thumb, str) and thumb.startswith("http"):
                    ready = await self._download_item(thumb, "preview.jpg", "photo")
                    ready.notice = "<i>[видео — только превью]</i>"
                    ready.groupable = False
                    return ready
                return _Prepared(notice="<i>[видео — не удалось получить]</i>",
                                 outcome=Outcome.TRANSIENT)

            if atype == "FILE":
                name = str(attach.get("name") or "file")
                size = attach.get("size") or 0
                if isinstance(size, int) and size > self.max_media_bytes:
                    # MAX states the size up front; fetching 50+ MB only to be
                    # refused at the limit wastes the download and the time.
                    return self._too_large(name, size)
                url = file_url(attach) or await self.client.resolve_file_url(
                    attach.get("fileId") or attach.get("id"),
                    msg.chat_id, msg.message_id)
                if not url:
                    size_text = f" ({human_size(size)})" if size else ""
                    return _Prepared(
                        notice=f"📎 <b>{escape(name)}</b>{size_text} "
                               "<i>— ссылка недоступна</i>",
                        outcome=Outcome.TRANSIENT)
                return await self._download_item(url, name, None)

            if atype == "AUDIO":
                url = attach.get("url")
                if not isinstance(url, str) or not url.startswith("http"):
                    url = await self.client.resolve_audio_url(
                        attach.get("audioId") or attach.get("id"),
                        msg.chat_id, msg.message_id, attach.get("token"))
                if not url:
                    return _Prepared(notice="<i>[аудио — не удалось получить]</i>",
                                     outcome=Outcome.TRANSIENT)
                return await self._download_item(url, "audio.ogg", "voice")

            if atype == "UNSUPPORTED" and attach.get("audioId") is not None:
                duration = attach.get("duration") or 0
                url = await self.client.resolve_audio_url(
                    attach.get("audioId"), msg.chat_id, msg.message_id,
                    attach.get("token"))
                if not url:
                    seconds = f" ({int(duration) // 1000}с)" if duration else ""
                    return _Prepared(
                        notice=f"🎙 <i>[голосовое сообщение{seconds} — MAX не "
                               "отдаёт ссылку на скачивание]</i>",
                        outcome=Outcome.PERMANENT)
                return await self._download_item(url, "voice.ogg", "voice")

            if atype == "STICKER":
                url = attach.get("url")
                if not isinstance(url, str) or not url.startswith("http"):
                    return _Prepared(notice="<i>[стикер]</i>",
                                     outcome=Outcome.PERMANENT)
                ready = await self._download_item(url, "sticker.webp", "sticker")
                ready.groupable = False
                return ready

            return _Prepared(notice=f"<i>[вложение: {escape(atype or 'unknown')}]</i>",
                             outcome=Outcome.PERMANENT)
        except Exception as exc:
            log.warning("Could not prepare %s attachment: %s", atype,
                        describe_exception(exc))
            return _Prepared(notice=f"<i>[{escape(atype.lower() or 'вложение')} — "
                                    "ошибка загрузки]</i>",
                             outcome=Outcome.TRANSIENT)

    async def _download_item(self, url: str, filename: str,
                             forced_kind: str | None) -> _Prepared:
        try:
            spooled = await self.client.download_media(
                url, filename=filename, max_bytes=self.max_media_bytes)
        except FileTooLarge as exc:
            log.info("Not forwarding %s: %s", safe_url(url),
                     describe_exception(exc))
            return self._too_large(filename, exc.size)
        except Exception as exc:
            log.info("Download failed for %s: %s", safe_url(url),
                     describe_exception(exc))
            return _Prepared(notice=f"📎 <b>{escape(filename)}</b> "
                                    "<i>— не удалось скачать, повторю позже</i>",
                             outcome=Outcome.TRANSIENT)

        kind = forced_kind
        if kind is None:
            with spooled.open() as handle:
                head = handle.read(16)
            kind = sniff_media_kind(spooled.filename, spooled.content_type, head)
        item = MediaItem(kind=kind, source=spooled, filename=spooled.filename)
        return _Prepared(items=[item], groupable=kind in GROUPABLE_KINDS)

    def _too_large(self, name: str, size: int | None) -> _Prepared:
        """A file Telegram will not take from a bot: say which and why.

        "[не удалось скачать вложение]" read like a network glitch, when the
        file was simply over the limit and would never arrive by retrying.
        """
        size_text = f" ({human_size(size)})" if size else ""
        return _Prepared(
            notice=f"📎 <b>{escape(name)}</b>{size_text} <i>— больше лимита "
                   f"пересылки ботом ({human_size(self.max_media_bytes)}), "
                   f"открой в MAX</i>",
            outcome=Outcome.PERMANENT)

    # ── context: header, quote, reply ──────────────────────────────

    async def _header(self, msg: MaxMessage) -> str:
        is_dm = self.resolver.is_dm(msg.chat_id)
        sender = escape(await self.resolver.resolve_user(msg.sender_id))
        if is_dm:
            return f"✉ <b>{sender}</b>" if sender else "✉"
        chat_label = escape(self.resolver.chat_name(msg.chat_id))
        if not sender:
            return f"💬 <b>{chat_label}</b>"
        return f"💬 <b>{chat_label}</b> | {sender}"

    def _forwarded_content(self, msg: MaxMessage) -> MaxMessage | None:
        """The original message, when this one is a forward."""
        link = msg.link or {}
        if str(link.get("type") or "").upper() != "FORWARD":
            return None
        inner = link.get("message")
        if not isinstance(inner, dict):
            inner = link
        return MaxMessage(
            chat_id=msg.chat_id,
            sender_id=inner.get("sender"),
            text=inner.get("text") or "",
            message_id=str(inner.get("id") or ""),
            attaches=[a for a in (inner.get("attaches") or [])
                      if isinstance(a, dict)],
            elements=[e for e in (inner.get("elements") or [])
                      if isinstance(e, dict)],
        )

    async def _quote_block(self, msg: MaxMessage) -> str:
        """One-line preview of what a reply or forward refers to."""
        link = msg.link or {}
        link_type = str(link.get("type") or "").upper()
        if link_type not in ("REPLY", "FORWARD"):
            return ""
        inner = link.get("message")
        if not isinstance(inner, dict):
            inner = link
        author_id = inner.get("sender") or link.get("sender")
        author = escape(await self.resolver.resolve_user(author_id)) if author_id else ""

        if link_type == "FORWARD":
            return f"↩️ <b>Переслано{f' от {author}' if author else ''}</b>"

        preview = str(inner.get("text") or "").strip().replace("\n", " ")
        if len(preview) > 120:
            preview = preview[:117] + "…"
        if not preview:
            attaches = meaningful_attaches(inner.get("attaches"))
            preview = f"[{len(attaches)} вложени(й)]" if attaches else "[без текста]"
        return (f"↩ <b>Ответ{f' на {author}' if author else ''}</b>: "
                f"<i>{escape(preview)}</i>")

    async def _previous_delivery(self, job: Job, msg: MaxMessage) -> int | None:
        """Telegram message ID of the previous revision of this MAX message."""
        if self.pipeline is None:
            return None
        for older in await self.pipeline.previous_revisions(
                chat=msg.chat_id, event_id=job.event_id, before_id=job.id):
            parts = await self.pipeline.parts_for(older.id)
            for part in parts.values():
                if part.status == PART_CONFIRMED and part.destination_ids:
                    return part.destination_ids[0]
        return None

    async def _reply_target(self, msg: MaxMessage) -> int | None:
        """Telegram message ID of the quoted message, if we delivered it.

        Using a native reply beats re-uploading the original's media for every
        reply, which both wasted traffic and duplicated pictures in the topic
        (audit M05).
        """
        link = msg.link or {}
        if str(link.get("type") or "").upper() != "REPLY":
            return None
        inner = link.get("message")
        if not isinstance(inner, dict):
            return None
        quoted_id = inner.get("id")
        if quoted_id is None or self.pipeline is None:
            return None
        job = await self.pipeline.find_job(chat=msg.chat_id,
                                          event_id=str(quoted_id))
        if job is None:
            return None
        parts = await self.pipeline.parts_for(job.id)
        for part in parts.values():
            if part.status == PART_CONFIRMED and part.destination_ids:
                return part.destination_ids[0]
        return None

    # ── bookkeeping ────────────────────────────────────────────────

    async def _parts(self, job_id: int) -> dict:
        if self.pipeline is None:
            return {}
        return await self.pipeline.parts_for(job_id)

    async def _record(self, job: Job, key: str, kind: str,
                      result: SendResult) -> None:
        if self.pipeline is None:
            return
        status = {
            Outcome.DELIVERED: PART_CONFIRMED,
            Outcome.UNCERTAIN: PART_UNCERTAIN,
            Outcome.PERMANENT: PART_FAILED,
            Outcome.TRANSIENT: PART_FAILED,
        }[result.outcome]
        if result.outcome is Outcome.PERMANENT and not result.message_ids:
            status = PART_SKIPPED if kind == "placeholder" else PART_FAILED
        await self.pipeline.record_part(
            job.id, key, kind, status,
            destination_ids=result.message_ids, error=result.error)
        if status == PART_CONFIRMED and result.message_ids:
            # Indexed both ways, so a reply in either direction can name the
            # message it answers without scanning the journal.
            await self.pipeline.link_messages(
                chat=job.chat_key, max_id=job.event_id,
                tg_ids=result.message_ids)

    async def _post_intro(self, chat_id: Any, thread_id: int) -> None:
        key = chat_key(chat_id)
        if key in self._intro_done:
            return
        self._intro_done.add(key)
        try:
            from app.tg_handler import post_topic_intro
            await post_topic_intro(self.sender.bot, self.topics.supergroup_id,
                                   self.client, self.resolver, chat_id, thread_id)
        except Exception as exc:
            # A missing profile card is cosmetic; it must never fail the message
            # that triggered it.
            log.info("Could not post the intro card for chat %s: %s", key,
                     describe_exception(exc))


def describe_message(msg: MaxMessage) -> str:
    """Log line for one message, with the body kept out by default."""
    return (f"id={msg.message_id} chat={msg.chat_key} "
            f"attaches={len(msg.attaches)} text={log_policy.preview(msg.text)}")


def lag_seconds(msg: MaxMessage) -> float | None:
    try:
        return time.time() - float(msg.timestamp) / 1000.0
    except (TypeError, ValueError):
        return None
