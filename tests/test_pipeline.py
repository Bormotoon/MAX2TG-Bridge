"""The scheduler: ordering, retries, recovery, and the failures that used to lose data."""

from __future__ import annotations

import asyncio

from app.errors import ConnectionLost, Outcome
from app.pipeline import DeliveryPipeline, RetryPolicy
from app.store import (
    STATUS_DELIVERED,
    STATUS_PERMANENT,
    STATUS_UNCERTAIN,
)
from tests.conftest import drain


class RecordingHandler:
    """Handler that records batches and replays a scripted list of outcomes."""

    def __init__(self, outcomes=None, group_size: int = 1):
        self.batches: list[list[int]] = []
        self.outcomes = list(outcomes or [])
        self.group_size = group_size
        self.raise_next: BaseException | None = None
        self.gate: asyncio.Event | None = None

    async def group(self, job, lookahead):
        return [job] + list(lookahead[: self.group_size - 1])

    async def process(self, jobs):
        self.batches.append([j.id for j in jobs])
        if self.gate is not None:
            await self.gate.wait()
        if self.raise_next is not None:
            exc, self.raise_next = self.raise_next, None
            raise exc
        outcome = self.outcomes.pop(0) if self.outcomes else Outcome.DELIVERED
        return {j.id: outcome for j in jobs}


async def make_pipeline(store, health, handler, **kwargs):
    retry = kwargs.pop("retry", RetryPolicy(max_attempts=3, base_delay=0.01,
                                            max_delay=0.05))
    pipeline = DeliveryPipeline(store, source="max", account="acc",
                               handler=handler, health=health, retry=retry,
                               **kwargs)
    await pipeline.start()
    return pipeline


class TestIngress:
    async def test_replay_is_not_enqueued_twice(self, store, health):
        handler = RecordingHandler()
        pipeline = await make_pipeline(store, health, handler)
        try:
            assert await pipeline.submit(chat=1, event_id="m", revision="r",
                                         payload={}) is True
            assert await pipeline.submit(chat=1, event_id="m", revision="r",
                                         payload={}) is False
            assert await drain(lambda: len(handler.batches) >= 1)
            await asyncio.sleep(0.05)
            assert sum(len(b) for b in handler.batches) == 1
        finally:
            await pipeline.stop(timeout=2)

    async def test_replay_counter_moves(self, store, health):
        pipeline = await make_pipeline(store, health, RecordingHandler())
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert health.counters.replays == 1
        finally:
            await pipeline.stop(timeout=2)


class TestFailureHandling:
    async def test_transient_failure_is_retried_not_dropped(self, store, health):
        """The core regression: a message that fails once must still arrive."""
        handler = RecordingHandler(outcomes=[Outcome.TRANSIENT, Outcome.DELIVERED])
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 2, timeout=3)
            job = await store.find_job(source="max", account="acc", chat=1,
                                       event_id="m")

            async def delivered():
                return (await store.get_job(job.id)).status == STATUS_DELIVERED

            for _ in range(200):
                if await delivered():
                    break
                await asyncio.sleep(0.01)
            assert await delivered()
        finally:
            await pipeline.stop(timeout=2)

    async def test_exhausted_attempts_park_the_job_and_free_the_chat(
            self, store, health):
        dead: list[str] = []

        async def on_dead(job, reason):
            dead.append(reason)

        handler = RecordingHandler(outcomes=[Outcome.TRANSIENT] * 10)
        pipeline = await make_pipeline(store, health, handler,
                                       on_dead_letter=on_dead)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: bool(dead), timeout=5)
            job = await store.find_job(source="max", account="acc", chat=1,
                                       event_id="m")
            assert (await store.get_job(job.id)).status == STATUS_PERMANENT
            # The chat is not wedged: the next message still goes through.
            handler.outcomes = [Outcome.DELIVERED]
            before = len(handler.batches)
            await pipeline.submit(chat=1, event_id="m2", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) > before, timeout=5)
            follow_up = await store.find_job(source="max", account="acc", chat=1,
                                             event_id="m2")
            for _ in range(200):
                if (await store.get_job(follow_up.id)).status == STATUS_DELIVERED:
                    break
                await asyncio.sleep(0.01)
            assert (await store.get_job(follow_up.id)).status == STATUS_DELIVERED
        finally:
            await pipeline.stop(timeout=2)

    async def test_permanent_failure_does_not_retry(self, store, health):
        handler = RecordingHandler(outcomes=[Outcome.PERMANENT])
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 1)
            await asyncio.sleep(0.1)
            assert len(handler.batches) == 1
        finally:
            await pipeline.stop(timeout=2)

    async def test_uncertain_is_parked_not_resent(self, store, health):
        """Telegram may have taken it; resending would duplicate the message."""
        handler = RecordingHandler(outcomes=[Outcome.UNCERTAIN])
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 1)
            await asyncio.sleep(0.1)
            job = await store.find_job(source="max", account="acc", chat=1,
                                       event_id="m")
            assert (await store.get_job(job.id)).status == STATUS_UNCERTAIN
            assert len(handler.batches) == 1
            assert health.counters.uncertain == 1
        finally:
            await pipeline.stop(timeout=2)

    async def test_connection_lost_is_transient_and_the_worker_survives(
            self, store, health):
        """R01: a reconnect mid-RPC must not kill the chat's worker for good."""
        handler = RecordingHandler()
        handler.raise_next = ConnectionLost("link dropped")
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 2, timeout=3)
            await pipeline.submit(chat=1, event_id="m2", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 3, timeout=3)
        finally:
            await pipeline.stop(timeout=2)

    async def test_handler_crash_restarts_the_worker(self, store, health):
        handler = RecordingHandler()
        handler.raise_next = RuntimeError("boom")
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 2, timeout=3)
        finally:
            await pipeline.stop(timeout=2)


class TestRetryCommand:
    async def test_retry_requeues_failed_jobs(self, store, health):
        handler = RecordingHandler(outcomes=[Outcome.PERMANENT])
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 1)
            await asyncio.sleep(0.05)
            handler.outcomes = [Outcome.DELIVERED]
            assert await pipeline.retry_failed() == 1
            assert await drain(lambda: len(handler.batches) >= 2, timeout=3)
        finally:
            await pipeline.stop(timeout=2)


class TestOrderingAndGrouping:
    async def test_one_chat_is_processed_sequentially(self, store, health):
        handler = RecordingHandler()
        handler.gate = asyncio.Event()
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="a", revision="r", payload={})
            await pipeline.submit(chat=1, event_id="b", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 1)
            await asyncio.sleep(0.05)
            # The second job cannot start while the first is still in flight.
            assert len(handler.batches) == 1
            handler.gate.set()
            assert await drain(lambda: len(handler.batches) >= 2, timeout=3)
            assert handler.batches[0][0] < handler.batches[1][0]
        finally:
            handler.gate.set()
            await pipeline.stop(timeout=2)

    async def test_separate_chats_progress_independently(self, store, health):
        handler = RecordingHandler()
        handler.gate = asyncio.Event()
        pipeline = await make_pipeline(store, health, handler)
        try:
            await pipeline.submit(chat=1, event_id="a", revision="r", payload={})
            await pipeline.submit(chat=2, event_id="a", revision="r", payload={})
            assert await drain(lambda: len(handler.batches) >= 2, timeout=3)
        finally:
            handler.gate.set()
            await pipeline.stop(timeout=2)

    async def test_grouping_batches_adjacent_jobs(self, store, health):
        handler = RecordingHandler(group_size=3)
        pipeline = await make_pipeline(store, health, handler)
        try:
            for i in range(3):
                await pipeline.submit(chat=1, event_id=str(i), revision="r",
                                      payload={})
            assert await drain(lambda: handler.batches and
                               len(handler.batches[0]) == 3, timeout=3)
        finally:
            await pipeline.stop(timeout=2)


class TestRecovery:
    async def test_a_restart_resumes_interrupted_jobs(self, store, health):
        """Crash between accept and deliver: the job comes back, once."""
        job = await store.enqueue(source="max", account="acc", chat=1,
                                  event_id="m", revision="r", payload={})
        await store.mark_processing(job.id)

        handler = RecordingHandler()
        pipeline = await make_pipeline(store, health, handler)
        try:
            assert await drain(lambda: handler.batches == [[job.id]], timeout=3)
            for _ in range(200):
                if (await store.get_job(job.id)).status == STATUS_DELIVERED:
                    break
                await asyncio.sleep(0.01)
            assert (await store.get_job(job.id)).status == STATUS_DELIVERED
            assert handler.batches == [[job.id]]   # delivered exactly once
        finally:
            await pipeline.stop(timeout=2)

    async def test_confirmed_parts_are_not_re_sent(self, store, health):
        job = await store.enqueue(source="max", account="acc", chat=1,
                                  event_id="m", revision="r", payload={})
        await store.record_part(job.id, "album", "album", "confirmed",
                                destination_ids=[5])
        await store.mark_processing(job.id)

        seen_parts = {}

        class PartAwareHandler(RecordingHandler):
            async def process(self, jobs):
                for j in jobs:
                    seen_parts[j.id] = await store.parts(j.id)
                return await super().process(jobs)

        handler = PartAwareHandler()
        pipeline = await make_pipeline(store, health, handler)
        try:
            assert await drain(lambda: job.id in seen_parts, timeout=3)
            assert seen_parts[job.id]["album"].destination_ids == [5]
        finally:
            await pipeline.stop(timeout=2)


class TestShutdown:
    async def test_stop_leaves_unfinished_work_recoverable(self, store, health):
        handler = RecordingHandler()
        handler.gate = asyncio.Event()
        pipeline = await make_pipeline(store, health, handler)
        await pipeline.submit(chat=1, event_id="m", revision="r", payload={})
        assert await drain(lambda: len(handler.batches) >= 1)

        await pipeline.stop(timeout=0.2)     # forces a cancel while in flight
        handler.gate.set()

        job = await store.find_job(source="max", account="acc", chat=1,
                                   event_id="m")
        assert (await store.get_job(job.id)).status != STATUS_DELIVERED
        recovered = await store.recover_processing(source="max", account="acc")
        assert recovered == 1
