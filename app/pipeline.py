"""Per-chat delivery scheduler on top of the durable journal.

The shape of the old bug: a DISPATCH was marked "seen" on arrival, handed to a
per-chat worker, and forgotten. If anything between there and Telegram failed,
the message was gone — MAX's replay was recognised as a duplicate and dropped.
Albums made it worse, since the timer that actually sent them ran in a task of
its own and could finish after the text that followed (audit D01, M01).

Here, one worker per chat owns everything for that chat: it takes the oldest
unfinished job from the database, decides on the spot whether the next few jobs
belong in the same album, sends the batch itself, and only then writes the
outcome back. Nothing is delivered by a task the scheduler does not await, so
ordering holds; nothing is marked done before it is done, so a crash resumes
instead of losing.

Failure policy is explicit rather than implied: a job that keeps failing blocks
its chat for ``max_attempts`` tries and is then parked in the dead-letter state
with a visible notice, because a single poisoned message silently stopping a
conversation forever is the worse of the two evils.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from app.errors import ConnectionLost, Outcome, describe_exception
from app.health import Health
from app.store import (
    STATUS_DELIVERED,
    STATUS_PERMANENT,
    STATUS_RETRYABLE,
    STATUS_UNCERTAIN,
    Job,
    Store,
    chat_key,
)

log = logging.getLogger(__name__)


class JobHandler(Protocol):
    """What the pipeline needs from whoever actually delivers a job."""

    async def group(self, job: Job, lookahead: list[Job]) -> list[Job]:
        """Pick the jobs to deliver together with ``job`` (always includes it)."""

    async def process(self, jobs: list[Job]) -> dict[int, Outcome]:
        """Deliver a batch and report each job's outcome."""


@dataclass
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 5.0
    max_delay: float = 600.0

    def next_delay(self, attempts: int) -> float:
        delay = min(self.base_delay * (2 ** max(0, attempts - 1)), self.max_delay)
        return delay * (0.7 + random.random() * 0.6)


class DeliveryPipeline:
    """Durable ingress plus one supervised worker per chat."""

    # How long a worker sits idle before retiring. Keeping one task per chat
    # ever seen alive forever was a slow leak on a long-running bridge (R06).
    IDLE_EXIT_SEC = 300.0
    # How many jobs ahead the grouper may look when assembling an album.
    LOOKAHEAD = 20

    def __init__(self, store: Store, *, source: str, account: str,
                 handler: JobHandler, health: Health,
                 retry: RetryPolicy | None = None,
                 on_dead_letter: Callable[[Job, str], Awaitable[None]] | None = None,
                 max_workers: int = 32):
        self._store = store
        self._source = source
        self._account = account
        self._handler = handler
        self._health = health
        self._retry = retry or RetryPolicy()
        self._on_dead_letter = on_dead_letter
        self._workers: dict[str, asyncio.Task] = {}
        self._wakeups: dict[str, asyncio.Event] = {}
        self._sem = asyncio.Semaphore(max_workers)
        self._stopping = asyncio.Event()
        self._started = False
        health.queue_probe = self._queue_probe

    # ── lifecycle ──────────────────────────────────────────────────

    async def start(self) -> None:
        """Recover whatever the previous run left behind and resume it."""
        recovered = await self._store.recover_processing(
            source=self._source, account=self._account)
        if recovered:
            log.warning("%d job(s) were interrupted mid-delivery and will be "
                        "resumed (already-confirmed parts are not re-sent)",
                        recovered)
        pruned = await self._store.prune()
        if pruned:
            log.info("Pruned %d delivered journal row(s) past the retention window",
                     pruned)
        for key in await self._store.active_chats(source=self._source,
                                                  account=self._account):
            self._ensure_worker(key)
        self._started = True
        log.info("Delivery pipeline (%s) started for %d chat(s) with pending work",
                 self._source, len(self._workers))

    async def stop(self, timeout: float = 20.0) -> None:
        """Stop accepting work and let in-flight deliveries finish briefly.

        Anything unfinished stays in the journal as ``processing`` and comes back
        as retryable on the next start, so a hard kill loses nothing that was
        already accepted.
        """
        self._stopping.set()
        for event in self._wakeups.values():
            event.set()
        tasks = list(self._workers.values())
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            log.warning("Delivery worker %s did not finish in %.0fs — cancelling",
                        task.get_name(), timeout)
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    # ── ingress ────────────────────────────────────────────────────

    async def submit(self, *, chat: Any, event_id: str, revision: str,
                     payload: dict) -> bool:
        """Record an event. Returns False when it is a replay we already hold.

        This is the deduplication point, and unlike the old in-memory cache it
        cannot forget an undelivered message: the row stays until it is
        delivered *and* the retention window has passed (audit D01/D04).
        """
        job = await self._store.enqueue(
            source=self._source, account=self._account, chat=chat,
            event_id=event_id, revision=revision, payload=payload)
        if job is None:
            self._health.counters.bump("replays")
            log.info("Replay of %s in chat %s — already journalled",
                     event_id, chat_key(chat))
            return False
        self._ensure_worker(chat_key(chat))
        return True

    async def previous_revisions(self, *, chat: Any, event_id: str,
                                 before_id: int) -> list[Job]:
        """Earlier journal rows for the same source message (i.e. pre-edit)."""
        return await self._store.job_history(
            source=self._source, account=self._account, chat=chat,
            event_id=event_id, before_id=before_id)

    async def parts_for(self, job_id: int) -> dict:
        return await self._store.parts(job_id)

    async def find_job(self, *, chat: Any, event_id: str) -> Job | None:
        """Latest journal row for one source message ID."""
        return await self._store.find_job(
            source=self._source, account=self._account, chat=chat,
            event_id=event_id)

    @property
    def store(self) -> Store:
        return self._store

    @property
    def account(self) -> str:
        return self._account

    @property
    def source(self) -> str:
        return self._source

    async def record_part(self, job_id: int, part_key: str, kind: str,
                          status: str, *, destination_ids=(),
                          error: str | None = None) -> None:
        await self._store.record_part(job_id, part_key, kind, status,
                                      destination_ids=destination_ids,
                                      error=error)

    async def link_messages(self, *, chat: Any, max_id: str, tg_ids) -> None:
        """Record the MAX ↔ Telegram identity of one delivered message."""
        await self._store.link_messages(account=self._account, chat=chat,
                                        max_id=max_id, tg_ids=tg_ids)

    async def max_id_for_tg(self, *, chat: Any, tg_id: int) -> str | None:
        return await self._store.max_id_for_tg(account=self._account, chat=chat,
                                               tg_id=tg_id)

    async def tg_id_for_max(self, *, chat: Any, max_id: str) -> int | None:
        return await self._store.tg_id_for_max(account=self._account, chat=chat,
                                               max_id=max_id)

    async def retry_failed(self, chat: Any | None = None, limit: int = 100) -> int:
        """Hand failed/uncertain jobs back to their workers (the ``/retry`` command)."""
        count = await self._store.requeue_failed(
            source=self._source, account=self._account, chat=chat, limit=limit)
        if count:
            keys = ([chat_key(chat)] if chat is not None
                    else await self._store.active_chats(source=self._source,
                                                        account=self._account))
            for key in keys:
                self._ensure_worker(key)
        return count

    async def stats(self) -> dict:
        return await self._store.stats(source=self._source)

    # ── workers ────────────────────────────────────────────────────

    def _ensure_worker(self, key: str) -> None:
        if self._stopping.is_set():
            return
        event = self._wakeups.get(key)
        if event is None:
            event = asyncio.Event()
            self._wakeups[key] = event
        event.set()
        task = self._workers.get(key)
        if task is not None and not task.done():
            return
        task = asyncio.create_task(self._supervise(key), name=f"chat:{key}")
        self._workers[key] = task
        self._health.worker_count = len(self._workers)

    async def _supervise(self, key: str) -> None:
        """Restart a worker that dies unexpectedly.

        The failure this exists for: a pending RPC aborted at reconnect used to
        propagate ``CancelledError`` through the worker, which then vanished
        while its queue stayed registered — that chat never forwarded another
        message until the process restarted (audit R01).
        """
        backoff = 1.0
        try:
            while not self._stopping.is_set():
                try:
                    await self._work(key)
                    return  # clean idle exit
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("Delivery worker for chat %s crashed: %s — "
                                  "restarting in %.0fs", key,
                                  describe_exception(exc), backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
        finally:
            if self._workers.get(key) is asyncio.current_task():
                self._workers.pop(key, None)
            self._health.worker_count = len(self._workers)

    async def _work(self, key: str) -> None:
        event = self._wakeups.setdefault(key, asyncio.Event())
        idle_since = time.monotonic()
        while not self._stopping.is_set():
            job = await self._store.next_job(source=self._source,
                                             account=self._account, chat=key)
            if job is None:
                wait_for = await self._idle_wait(key, idle_since)
                if wait_for is None:
                    return  # retired
                event.clear()
                try:
                    await asyncio.wait_for(event.wait(), timeout=wait_for)
                except TimeoutError:
                    pass
                continue

            idle_since = time.monotonic()
            async with self._sem:
                await self._run_batch(key, job)

    async def _idle_wait(self, key: str, idle_since: float) -> float | None:
        """How long to sleep with nothing to do, or ``None`` to retire."""
        pending = await self._store.peek_jobs(
            source=self._source, account=self._account, chat=key,
            after_id=0, limit=1)
        if pending:
            job = pending[0]
            if job.next_retry_at:
                return max(1.0, job.next_retry_at - time.time())
            return 1.0
        if time.monotonic() - idle_since > self.IDLE_EXIT_SEC:
            return None
        return 30.0

    async def _run_batch(self, key: str, job: Job) -> None:
        lookahead = await self._store.peek_jobs(
            source=self._source, account=self._account, chat=key,
            after_id=job.id, limit=self.LOOKAHEAD)
        try:
            jobs = await self._handler.group(job, lookahead)
        except Exception:
            log.exception("Grouping failed for chat %s — handling job %s alone",
                          key, job.id)
            jobs = [job]
        if not jobs:
            jobs = [job]

        for item in jobs:
            await self._store.mark_processing(item.id)

        started = time.monotonic()
        try:
            outcomes = await self._handler.process(jobs)
        except ConnectionLost as exc:
            # Expected during a reconnect: the batch goes back to the queue with
            # its confirmed parts intact, and the worker survives.
            log.info("Batch in chat %s hit a MAX reconnect: %s", key,
                     describe_exception(exc))
            outcomes = {item.id: Outcome.TRANSIENT for item in jobs}
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Batch in chat %s failed: %s", key, describe_exception(exc))
            outcomes = {item.id: Outcome.TRANSIENT for item in jobs}

        elapsed = time.monotonic() - started
        if elapsed >= 60:
            log.warning("Batch of %d job(s) in chat %s took %.0fs — everything "
                        "behind it waited that long", len(jobs), key, elapsed)

        for item in jobs:
            await self._settle(item, outcomes.get(item.id, Outcome.TRANSIENT))

    async def _settle(self, job: Job, outcome: Outcome) -> None:
        if outcome is Outcome.DELIVERED:
            await self._store.finish_job(job.id, STATUS_DELIVERED)
            self._health.note_delivery()
            return

        if outcome is Outcome.UNCERTAIN:
            # Telegram may have taken it. Re-sending risks a duplicate and there
            # is no idempotency key, so it is parked for a human decision via
            # /status and /retry rather than replayed automatically (audit D06).
            await self._store.finish_job(job.id, STATUS_UNCERTAIN,
                                         error="result unknown after timeout")
            self._health.counters.bump("uncertain")
            log.warning("Job %s (message %s) is uncertain — not retried "
                        "automatically; use /retry to force it",
                        job.id, job.event_id)
            await self._dead_letter(job, "результат отправки неизвестен")
            return

        attempts = job.attempts + 1
        if outcome is Outcome.PERMANENT or attempts >= self._retry.max_attempts:
            await self._store.finish_job(
                job.id, STATUS_PERMANENT,
                error=f"gave up after {attempts} attempt(s)")
            self._health.counters.bump("failed")
            log.error("Job %s (message %s in chat %s) failed permanently after "
                      "%d attempt(s) — parked, the chat continues",
                      job.id, job.event_id, job.chat_key, attempts)
            await self._dead_letter(job, f"не доставлено за {attempts} попыт(ок)")
            return

        delay = self._retry.next_delay(attempts)
        await self._store.finish_job(job.id, STATUS_RETRYABLE,
                                     error="transient failure",
                                     next_retry_at=time.time() + delay)
        log.info("Job %s will be retried in %.0fs (attempt %d/%d)",
                 job.id, delay, attempts, self._retry.max_attempts)

    async def _dead_letter(self, job: Job, reason: str) -> None:
        if self._on_dead_letter is None:
            return
        try:
            await self._on_dead_letter(job, reason)
        except Exception:
            log.exception("Dead-letter notification failed for job %s", job.id)

    # ── observability ──────────────────────────────────────────────

    def _queue_probe(self) -> dict[str, Any]:
        return {
            "workers": len(self._workers),
            "chats": sorted(self._workers.keys())[:20],
            "started": self._started,
        }
