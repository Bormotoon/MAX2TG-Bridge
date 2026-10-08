"""Telegram → MAX worker: grouping, uploads, and honest outcomes."""

from __future__ import annotations

import pytest

from app.errors import ConnectionLost, MaxRpcError, NotReady, Outcome
from app.health import Health
from app.pipeline import DeliveryPipeline, RetryPolicy
from app.store import PART_CONFIRMED, PART_UNCERTAIN, Job
from app.tg_outbound import TelegramToMaxHandler


class FakeMaxClient:
    def __init__(self):
        self.ready = True
        self.sent: list[dict] = []
        self.send_error: BaseException | None = None
        self.edits: list[dict] = []
        self.response = {"message": {"id": "max-1"}}

    async def send_message(self, chat_id, text="", elements=None, attaches=None,
                           cid=None, link=None):
        if self.send_error is not None:
            error, self.send_error = self.send_error, None
            raise error
        self.sent.append({"chat_id": chat_id, "text": text,
                          "attaches": list(attaches or []), "cid": cid,
                          "link": link})
        return self.response

    async def cmd(self, opcode, payload, timeout=None):
        self.edits.append({"opcode": opcode, "payload": payload})
        return {"message": {"id": "max-edited"}}

    async def upload_photo(self, source, **kwargs):
        return {"_type": "PHOTO", "photoToken": "tok"}

    async def upload_file(self, source, **kwargs):
        return {"_type": "FILE", "fileId": 1}

    async def upload_audio(self, source, **kwargs):
        return {"_type": "FILE", "fileId": 2}


class FakeTgBot:
    def __init__(self):
        self.reactions: list[dict] = []
        self.messages: list[dict] = []

    async def set_message_reaction(self, **kwargs):
        self.reactions.append(kwargs)

    async def send_message(self, **kwargs):
        self.messages.append(kwargs)

    async def get_file(self, file_id):
        raise AssertionError("no test here downloads a real file")


@pytest.fixture
def outbound(store, spool):
    client = FakeMaxClient()
    bot = FakeTgBot()
    handler = TelegramToMaxHandler(client, bot, spool, Health())
    pipeline = DeliveryPipeline(store, source="telegram", account="acc",
                                handler=handler, health=handler.health,
                                retry=RetryPolicy(max_attempts=2, base_delay=0.01))
    handler.bind(pipeline)
    return handler


def payload(**overrides):
    base = {
        "max_chat_id": -42, "tg_chat_id": -100500, "tg_message_id": 9,
        "thread_id": 7, "text": "привет", "elements": [], "media": None,
        "media_group_id": None, "reply_to": None, "edited": False,
    }
    base.update(overrides)
    return base


async def enqueue(store, event_id="9", **overrides) -> Job:
    return await store.enqueue(source="telegram", account="acc", chat=-42,
                               event_id=event_id, revision=f"rev-{event_id}",
                               payload=payload(**overrides))


class TestGrouping:
    async def test_single_messages_are_not_grouped(self, outbound, store):
        job = await enqueue(store)
        assert await outbound.group(job, []) == [job]

    async def test_an_album_is_grouped(self, outbound, store):
        lead = await enqueue(store, "1", media_group_id="g1")
        rest = [await enqueue(store, "2", media_group_id="g1"),
                await enqueue(store, "3", media_group_id="g1")]
        assert len(await outbound.group(lead, rest)) == 3

    async def test_a_different_album_ends_the_group(self, outbound, store):
        lead = await enqueue(store, "1", media_group_id="g1")
        rest = [await enqueue(store, "2", media_group_id="g2")]
        assert len(await outbound.group(lead, rest)) == 1

    async def test_an_album_is_capped_at_ten(self, outbound, store):
        lead = await enqueue(store, "1", media_group_id="g1")
        rest = [await enqueue(store, str(i), media_group_id="g1")
                for i in range(2, 20)]
        assert len(await outbound.group(lead, rest)) <= 10


class TestDelivery:
    async def test_text_is_sent_and_confirmed(self, outbound, store):
        job = await enqueue(store)
        result = await outbound.process([job])
        assert result == {job.id: Outcome.DELIVERED}
        assert outbound.client.sent[0]["text"] == "привет"
        parts = await outbound.pipeline.parts_for(job.id)
        assert parts["send"].status == PART_CONFIRMED
        assert parts["send"].destination_ids == ["max-1"]

    async def test_a_confirmed_message_is_not_sent_twice(self, outbound, store):
        job = await enqueue(store)
        await outbound.process([job])
        await outbound.process([job])
        assert len(outbound.client.sent) == 1

    async def test_the_reaction_confirms_delivery_to_the_user(self, outbound, store):
        job = await enqueue(store)
        await outbound.process([job])
        assert outbound.bot.reactions[0]["message_id"] == 9

    async def test_no_connection_is_transient_not_lost(self, outbound, store):
        outbound.client.ready = False
        job = await enqueue(store)
        assert await outbound.process([job]) == {job.id: Outcome.TRANSIENT}

    async def test_connection_lost_mid_send_is_transient(self, outbound, store):
        outbound.client.send_error = ConnectionLost("gone")
        job = await enqueue(store)
        assert await outbound.process([job]) == {job.id: Outcome.TRANSIENT}

    async def test_not_ready_is_transient(self, outbound, store):
        outbound.client.send_error = NotReady("not yet")
        job = await enqueue(store)
        assert await outbound.process([job]) == {job.id: Outcome.TRANSIENT}

    async def test_a_max_error_is_permanent_and_reported(self, outbound, store):
        outbound.client.send_error = MaxRpcError(64, {"message": "нельзя"})
        job = await enqueue(store)
        assert await outbound.process([job]) == {job.id: Outcome.PERMANENT}
        assert "нельзя" in outbound.bot.messages[0]["text"]

    async def test_a_timeout_is_uncertain_and_says_so(self, outbound, store):
        """Resending could duplicate; the user is told rather than guessed for."""
        outbound.client.send_error = TimeoutError()
        job = await enqueue(store)
        assert await outbound.process([job]) == {job.id: Outcome.UNCERTAIN}
        parts = await outbound.pipeline.parts_for(job.id)
        assert parts["send"].status == PART_UNCERTAIN
        assert "неизвестен" in outbound.bot.messages[0]["text"]

    async def test_an_empty_job_is_permanent(self, outbound, store):
        job = await enqueue(store, text="", media=None)
        assert await outbound.process([job]) == {job.id: Outcome.PERMANENT}

    async def test_a_missing_chat_is_permanent(self, outbound, store):
        job = await store.enqueue(source="telegram", account="acc", chat=-42,
                                  event_id="x", revision="r",
                                  payload=payload(max_chat_id=None))
        assert await outbound.process([job]) == {job.id: Outcome.PERMANENT}


class TestNonFileMedia:
    async def test_location_becomes_text(self, outbound, store):
        job = await enqueue(store, text="", media={"kind": "location",
                                                   "lat": 0.0, "lon": 0.0})
        await outbound.process([job])
        assert "0.0, 0.0" in outbound.client.sent[0]["text"]

    async def test_contact_becomes_text(self, outbound, store):
        job = await enqueue(store, text="", media={
            "kind": "contact", "phone": "+7000", "first_name": "Алиса",
            "last_name": None})
        await outbound.process([job])
        assert "Алиса" in outbound.client.sent[0]["text"]

    async def test_an_animated_sticker_becomes_its_emoji(self, outbound, store):
        job = await enqueue(store, text="", media={"kind": "sticker_text",
                                                   "emoji": "🔥"})
        await outbound.process([job])
        assert outbound.client.sent[0]["text"] == "🔥"

    async def test_an_oversized_file_is_explained_not_dropped(self, outbound, store):
        """20 MB is the getFile limit; the user should learn why nothing arrived."""
        job = await enqueue(store, text="", media={
            "kind": "document", "file_id": "f", "file_size": 30 * 1024 * 1024,
            "filename": "big.zip", "mime": "application/zip"})
        await outbound.process([job])
        assert "20 МБ" in outbound.client.sent[0]["text"]


class TestStableCid:
    def test_the_same_job_keeps_its_client_id(self, outbound):
        job = Job(id=5, source="telegram", account="a", chat_key="-1",
                  event_id="e", revision="r", payload={}, status="received",
                  attempts=0, next_retry_at=None, last_error=None,
                  received_at=0, updated_at=0)
        assert outbound._stable_cid(job) == outbound._stable_cid(job)

    def test_different_jobs_differ(self, outbound):
        def job(job_id):
            return Job(id=job_id, source="telegram", account="a", chat_key="-1",
                       event_id="e", revision="r", payload={}, status="received",
                       attempts=0, next_retry_at=None, last_error=None,
                       received_at=0, updated_at=0)
        assert outbound._stable_cid(job(1)) != outbound._stable_cid(job(2))


class TestEdits:
    async def test_an_edit_updates_the_original_max_message(self, outbound, store):
        original = await enqueue(store, "9")
        await outbound.process([original])

        edited = await store.enqueue(
            source="telegram", account="acc", chat=-42, event_id="9",
            revision="rev-edited", payload=payload(text="исправлено", edited=True))
        result = await outbound.process([edited])
        assert result == {edited.id: Outcome.DELIVERED}
        assert outbound.client.edits[0]["payload"]["messageId"] == "max-1"
        assert len(outbound.client.sent) == 1     # not sent again as new

    async def test_an_edit_of_an_unknown_message_is_sent_as_new(self, outbound,
                                                                store):
        edited = await store.enqueue(
            source="telegram", account="acc", chat=-42, event_id="99",
            revision="r", payload=payload(text="что-то", edited=True))
        await outbound.process([edited])
        assert outbound.client.edits == []
        assert len(outbound.client.sent) == 1


class TestStableClientId:
    """The MAX ``cid`` must identify a logical send, not a process run.

    It exists so a retry of one send is at least *recognisable* as such to MAX
    (audit D06). The obvious implementation used ``hash()``, which Python salts
    per process — so the id stayed stable across every retry except the one that
    follows a restart, which is exactly when a duplicate is most likely.
    """

    @staticmethod
    def _job(job_id: int = 7, revision: str = "rev-a") -> Job:
        return Job(id=job_id, source="telegram", account="acc", chat_key="-1",
                   event_id="100", revision=revision, payload={}, status="received",
                   attempts=0, next_retry_at=None, last_error=None,
                   received_at=0.0, updated_at=0.0)

    def test_same_job_gives_the_same_id(self):
        assert (TelegramToMaxHandler._stable_cid(self._job())
                == TelegramToMaxHandler._stable_cid(self._job()))

    def test_a_new_revision_gives_a_new_id(self):
        assert (TelegramToMaxHandler._stable_cid(self._job(revision="rev-a"))
                != TelegramToMaxHandler._stable_cid(self._job(revision="rev-b")))

    def test_the_id_survives_a_restart(self):
        """Re-run under a different PYTHONHASHSEED and compare."""
        import subprocess
        import sys

        script = (
            "from app.tg_outbound import TelegramToMaxHandler as H;"
            "from app.store import Job;"
            "print(H._stable_cid(Job(id=7, source='telegram', account='acc',"
            " chat_key='-1', event_id='100', revision='rev-a', payload={},"
            " status='received', attempts=0, next_retry_at=None,"
            " last_error=None, received_at=0.0, updated_at=0.0)))"
        )
        seen = set()
        for seed in ("1", "2"):
            out = subprocess.run([sys.executable, "-c", script],
                                 capture_output=True, text=True, check=True,
                                 env={"PYTHONHASHSEED": seed, "PATH": "/usr/bin",
                                      "PYTHONPATH": "."})
            seen.add(out.stdout.strip())
        assert len(seen) == 1, f"the id changed between processes: {seen}"
        assert seen == {str(TelegramToMaxHandler._stable_cid(self._job()))}

    def test_the_id_fits_the_field(self):
        assert 0 <= TelegramToMaxHandler._stable_cid(self._job()) < 10 ** 15


@pytest.fixture
def outbound_with_replies(store, spool):
    client = FakeMaxClient()
    handler = TelegramToMaxHandler(client, FakeTgBot(), spool, Health(),
                                   reply_link=True)
    pipeline = DeliveryPipeline(store, source="telegram", account="acc",
                                handler=handler, health=handler.health,
                                retry=RetryPolicy(max_attempts=2, base_delay=0.01))
    handler.bind(pipeline)
    return handler


class TestReplyMapping:
    """A reply in a topic has to name the MAX message it answers.

    The resolution below is exact and tested. The wire format of the ``link``
    object it produces is *not* confirmed, which is why it sits behind
    MAX_REPLY_LINK and defaults to off (audit M11, T08).
    """

    async def test_off_by_default_nothing_is_sent(self, outbound, store):
        await store.link_messages(account="acc", chat=-42, max_id="max-7",
                                  tg_ids=[9])
        job = await enqueue(store, "10", reply_to=9)
        await outbound.process([job])
        assert outbound.client.sent[-1]["link"] is None

    async def test_a_reply_to_a_known_message_resolves(self, outbound_with_replies,
                                                       store):
        handler = outbound_with_replies
        await store.link_messages(account="acc", chat=-42, max_id="max-7",
                                  tg_ids=[9])
        job = await enqueue(store, "10", reply_to=9)
        await handler.process([job])
        assert handler.client.sent[-1]["link"] == {"type": "REPLY",
                                                   "messageId": "max-7"}

    async def test_a_reply_to_an_unknown_message_sends_without_a_link(
            self, outbound_with_replies, store):
        handler = outbound_with_replies
        job = await enqueue(store, "10", reply_to=4242)
        await handler.process([job])
        assert handler.client.sent[-1]["link"] is None
        assert handler.client.sent[-1]["text"] == "привет"

    async def test_a_message_that_is_not_a_reply_carries_no_link(
            self, outbound_with_replies, store):
        handler = outbound_with_replies
        job = await enqueue(store, "10")
        await handler.process([job])
        assert handler.client.sent[-1]["link"] is None

    async def test_a_reply_in_another_chat_does_not_resolve(
            self, outbound_with_replies, store):
        """Telegram IDs are per-supergroup; MAX IDs are per-chat."""
        handler = outbound_with_replies
        await store.link_messages(account="acc", chat=-99, max_id="max-7",
                                  tg_ids=[9])
        job = await enqueue(store, "10", reply_to=9)
        await handler.process([job])
        assert handler.client.sent[-1]["link"] is None

    async def test_a_delivered_message_becomes_quotable_itself(self, outbound,
                                                               store):
        """Replying to your own earlier message has to work too."""
        job = await enqueue(store, "9")
        await outbound.process([job])
        assert await store.max_id_for_tg(account="acc", chat=-42,
                                         tg_id=9) == "max-1"
