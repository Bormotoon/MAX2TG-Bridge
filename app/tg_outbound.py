"""Telegram → MAX: the worker half of the outbound journal.

The Telegram handler only writes the update down and returns; everything slow
happens here, on the same per-chat scheduler the inbound direction uses. That is
what stops one large upload from blocking commands in every other topic
(audit S02) and what makes a crash mid-upload resumable instead of silent.

A Telegram album (``media_group_id``) is delivered to MAX as *one* message with
several attachments, rather than one message per file as before (audit M11).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Any

from telegram.error import TelegramError

from app.errors import (
    ConnectionLost,
    MaxRpcError,
    NotReady,
    Outcome,
    describe_exception,
)
from app.health import Health
from app.max_client import MaxClient
from app.net import MediaSpool, SpoolFile
from app.pipeline import DeliveryPipeline
from app.store import PART_CONFIRMED, PART_FAILED, PART_UNCERTAIN, Job

log = logging.getLogger(__name__)

SOURCE = "telegram"

# Telegram caps what a bot may download through getFile.
TG_GETFILE_LIMIT = 20 * 1024 * 1024


class TelegramToMaxHandler:
    """Uploads Telegram content into MAX and reports the result in the topic."""

    def __init__(self, client: MaxClient, bot, spool: MediaSpool, health: Health,
                 *, reply_link: bool = False):
        self.client = client
        self.bot = bot
        self.spool = spool
        self.health = health
        self.reply_link = reply_link
        self.pipeline: DeliveryPipeline | None = None

    def bind(self, pipeline: DeliveryPipeline) -> None:
        self.pipeline = pipeline

    # ── grouping ───────────────────────────────────────────────────

    async def group(self, job: Job, lookahead: list[Job]) -> list[Job]:
        group_id = job.payload.get("media_group_id")
        if not group_id:
            return [job]
        batch = [job]
        for candidate in lookahead:
            if candidate.payload.get("media_group_id") != group_id:
                break
            batch.append(candidate)
            if len(batch) >= 10:
                break
        return batch

    # ── delivery ───────────────────────────────────────────────────

    async def process(self, jobs: list[Job]) -> dict[int, Outcome]:
        lead = jobs[0]
        max_chat_id = lead.payload.get("max_chat_id")
        if max_chat_id is None:
            return {job.id: Outcome.PERMANENT for job in jobs}

        if not self.client.ready:
            # Nothing is lost: the job stays in the journal and the worker
            # retries once the link is back.
            return {job.id: Outcome.TRANSIENT for job in jobs}

        parts = await self._parts(lead.id)
        if parts.get("send") and parts["send"].status == PART_CONFIRMED:
            return {job.id: Outcome.DELIVERED for job in jobs}

        spooled: list[SpoolFile] = []
        attaches: list[dict] = []
        notes: list[str] = []
        try:
            for job in jobs:
                media = job.payload.get("media")
                if not media:
                    continue
                attach, note, downloaded = await self._prepare(media, max_chat_id)
                if downloaded is not None:
                    spooled.append(downloaded)
                if attach is not None:
                    attaches.append(attach)
                elif note:
                    notes.append(note)

            text = lead.payload.get("text") or ""
            if notes:
                text = (text + "\n" + "\n".join(notes)).strip()
            if not text and not attaches:
                return {job.id: Outcome.PERMANENT for job in jobs}

            outcome = await self._send(lead, jobs, max_chat_id, text, attaches)
        finally:
            for item in spooled:
                item.release()
        return outcome

    async def _send(self, lead: Job, jobs: list[Job], max_chat_id: Any,
                    text: str, attaches: list[dict]) -> dict[int, Outcome]:
        # The client id stays the same across retries of one logical send, so a
        # duplicate is at least recognisable — MAX's server-side idempotency is
        # unverified, so this is not a guarantee (audit D06).
        cid = self._stable_cid(lead)
        edited_target = await self._edit_target(lead)
        link = await self._reply_link(lead, max_chat_id)
        try:
            if edited_target is not None:
                resp = await self.client.cmd(67, {
                    "chatId": max_chat_id,
                    "messageId": edited_target,
                    "message": {"text": text,
                                "elements": lead.payload.get("elements") or []},
                })
            else:
                resp = await self.client.send_message(
                    max_chat_id, text,
                    elements=lead.payload.get("elements") or [],
                    attaches=attaches, cid=cid, link=link)
        except MaxRpcError as exc:
            await self._report(lead, f"⚠️ MAX: {exc.describe()}")
            await self._record(lead, PART_FAILED, error=exc.describe())
            return {job.id: Outcome.PERMANENT for job in jobs}
        except (NotReady, ConnectionLost):
            return {job.id: Outcome.TRANSIENT for job in jobs}
        except TimeoutError:
            # MAX may have taken it. Do not resend on its own.
            await self._report(
                lead, "⚠️ MAX не подтвердил доставку — статус неизвестен, "
                      "повтор может создать дубль (см. /status).")
            await self._record(lead, PART_UNCERTAIN, error="timeout")
            return {job.id: Outcome.UNCERTAIN for job in jobs}

        message_id = self._message_id(resp)
        await self._record(lead, PART_CONFIRMED,
                           ids=[message_id] if message_id else [])
        await self._react(lead)
        self.health.note_delivery()
        return {job.id: Outcome.DELIVERED for job in jobs}

    # ── media ──────────────────────────────────────────────────────

    async def _prepare(self, media: dict, max_chat_id: Any
                       ) -> tuple[dict | None, str, SpoolFile | None]:
        kind = media.get("kind")

        if kind == "location":
            return None, f"📍 {media.get('lat')}, {media.get('lon')}", None
        if kind == "contact":
            name = " ".join(filter(None, [media.get("first_name"),
                                          media.get("last_name")]))
            return None, f"👤 {name} — {media.get('phone')}", None
        if kind == "sticker_text":
            return None, media.get("emoji") or "🙂", None

        size = media.get("file_size") or 0
        if size > TG_GETFILE_LIMIT:
            return None, (f"[файл {size // (1024 * 1024)} МБ не передан: "
                          "Bot API не отдаёт боту файлы больше 20 МБ]"), None

        spooled = await self._download(media)
        if spooled is None:
            return None, "[не удалось скачать вложение из Telegram]", None

        filename = media.get("filename") or "file"
        mimetype = media.get("mime") or "application/octet-stream"
        if kind == "photo":
            attach = await self.client.upload_photo(
                spooled, chat_id=max_chat_id, filename=filename, mimetype=mimetype)
        elif kind in ("voice", "audio"):
            attach = await self.client.upload_audio(
                spooled, chat_id=max_chat_id, filename=filename, mimetype=mimetype)
        else:
            attach = await self.client.upload_file(
                spooled, chat_id=max_chat_id, filename=filename, mimetype=mimetype)

        if attach is None:
            return None, "[не удалось загрузить вложение в MAX]", spooled
        return attach, "", spooled

    async def _download(self, media: dict) -> SpoolFile | None:
        """Pull a Telegram file into the spool rather than into memory."""
        try:
            tg_file = await self.bot.get_file(media["file_id"])
        except TelegramError as exc:
            log.warning("get_file failed: %s", describe_exception(exc))
            return None

        fd, path = self.spool.new_temp()
        reserved = 0
        try:
            os.close(fd)
            await tg_file.download_to_drive(custom_path=path)
            size = await asyncio.to_thread(os.path.getsize, path)
            if size > TG_GETFILE_LIMIT:
                raise ValueError(f"file is {size} bytes, over the getFile limit")
            await self.spool.reserve(size)
            reserved = size
            return SpoolFile(path, media.get("filename") or "file",
                             media.get("mime") or "", size, self.spool)
        except Exception as exc:
            log.warning("Downloading a Telegram file failed: %s",
                        describe_exception(exc))
            if reserved:
                self.spool._release(path, reserved)
            else:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            return None

    # ── bookkeeping and feedback ───────────────────────────────────

    @staticmethod
    def _stable_cid(job: Job) -> int:
        """A client id derived from the job, identical on every retry.

        Hashed explicitly rather than with ``hash()``: that is salted per
        process, so the obvious version of this changed identity across a
        restart — exactly the retry where a duplicate is most likely and where
        a stable id is the only thing that could make one recognisable
        (audit D06).
        """
        digest = hashlib.sha256(f"{job.id}:{job.revision}".encode()).digest()
        return int.from_bytes(digest[:8], "big") % (10 ** 15)

    @staticmethod
    def _message_id(resp: Any) -> Any:
        if not isinstance(resp, dict):
            return None
        message = resp.get("message")
        if isinstance(message, dict):
            return message.get("id")
        return resp.get("messageId")

    async def _reply_link(self, job: Job, max_chat_id: Any) -> dict | None:
        """Build the ``link`` object that makes this send a reply in MAX.

        **The wire format is not confirmed.** We know the *inbound* shape,
        because we parse it: a DISPATCH carries
        ``link = {"type": "REPLY", "message": {"id": ...}}``. What ``SEND_MESSAGE``
        accepts on the way out has never been captured from the web client, and
        a payload MAX does not recognise answers ``proto.payload`` — which, for
        several opcodes, closes the whole socket (audit T08). Sending a guess on
        every reply would therefore mean reconnecting on every reply.

        So this stays behind ``MAX_REPLY_LINK`` and is off by default. The
        resolution below it — Telegram message ID to MAX message ID — is exact
        and tested; only the last line is a guess. To finish this:

        1. Capture web.max.ru sending a reply (AUDIT.md §14.6 has the method).
        2. Correct the dict returned here to match.
        3. Default the flag to true.
        """
        if not self.reply_link or self.pipeline is None:
            return None
        reply_to = job.payload.get("reply_to")
        if not reply_to:
            return None
        max_id = await self.pipeline.max_id_for_tg(chat=max_chat_id,
                                                   tg_id=int(reply_to))
        if max_id is None:
            log.debug("Telegram message %s has no known MAX counterpart — "
                      "sending without a reply link", reply_to)
            return None
        return {"type": "REPLY", "messageId": str(max_id)}

    async def _edit_target(self, job: Job) -> Any | None:
        """MAX message ID a Telegram edit should update, if we know it."""
        if not job.payload.get("edited") or self.pipeline is None:
            return None
        previous = await self.pipeline.previous_revisions(
            chat=job.payload.get("max_chat_id"), event_id=job.event_id,
            before_id=job.id)
        for old in previous:
            parts = await self.pipeline.parts_for(old.id)
            part = parts.get("send")
            if part and part.status == PART_CONFIRMED and part.destination_ids:
                return part.destination_ids[0]
        return None

    async def _parts(self, job_id: int) -> dict:
        if self.pipeline is None:
            return {}
        return await self.pipeline.parts_for(job_id)

    async def _record(self, job: Job, status: str, *, ids=(),
                      error: str | None = None) -> None:
        if self.pipeline is None:
            return
        confirmed = [i for i in ids if i]
        await self.pipeline.record_part(job.id, "send", "max-message", status,
                                        destination_ids=confirmed,
                                        error=error)
        tg_id = job.payload.get("tg_message_id")
        if status == PART_CONFIRMED and confirmed and tg_id:
            # Our own outgoing message is quotable too: a reply to something the
            # owner typed in the topic has to resolve just like a reply to
            # something that arrived from MAX.
            await self.pipeline.link_messages(
                chat=job.payload.get("max_chat_id"), max_id=str(confirmed[0]),
                tg_ids=[tg_id])

    async def _react(self, job: Job) -> None:
        """Mark the Telegram message as confirmed by MAX."""
        try:
            await self.bot.set_message_reaction(
                chat_id=job.payload["tg_chat_id"],
                message_id=job.payload["tg_message_id"],
                reaction="👀")
        except Exception:
            # Reactions need "All emoji" enabled in the group; not being able to
            # set one is cosmetic.
            log.debug("Could not set the delivery reaction", exc_info=True)

    async def _report(self, job: Job, text: str) -> None:
        try:
            await self.bot.send_message(
                chat_id=job.payload["tg_chat_id"], text=text,
                message_thread_id=job.payload.get("thread_id"),
                reply_to_message_id=job.payload.get("tg_message_id"))
        except Exception:
            log.info("Could not report an outbound failure to Telegram",
                     exc_info=True)
