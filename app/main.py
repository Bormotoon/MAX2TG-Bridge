"""Startup, wiring and a shutdown that actually finishes what it started.

The old entry point built the Telegram client outside its own ``try``, ran the
loop by hand, and relied on ``KeyboardInterrupt`` to unwind — so a failure during
startup skipped cleanup entirely, and ``SIGTERM`` from Docker or systemd was not
handled at all: the container was killed with queues in memory and nothing
flushed (audit R08).

Everything now hangs off an :class:`~contextlib.AsyncExitStack`, both signals are
handled, and stopping means: stop accepting new work, let in-flight deliveries
finish within a budget, then close the clients. Anything still unfinished stays
in the journal and resumes on the next start — which is also what happens after a
``SIGKILL`` or an OOM, because the journal is the source of truth rather than
anything held in memory.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack
from logging.handlers import RotatingFileHandler

import aiohttp
from telegram import Update

from app import logging_utils
from app.config import ConfigError, Settings, load_settings, log_notes
from app.errors import StoreCorrupted, describe_exception
from app.health import Health, LinkState, format_duration
from app.healthcheck import health_path, write_snapshot
from app.max_client import _BROWSER_HEADERS, _HTTP_HEADERS, MaxClient
from app.max_listener import SOURCE as MAX_SOURCE
from app.max_listener import MaxToTelegramHandler
from app.net import FetchPolicy, MediaSpool, SafeFetcher
from app.pipeline import DeliveryPipeline, RetryPolicy
from app.resolver import ContactResolver
from app.store import Store
from app.tg_handler import Bridge, build_tg_app
from app.tg_outbound import SOURCE as TG_SOURCE
from app.tg_outbound import TelegramToMaxHandler
from app.tg_sender import StatusNotifier, TelegramSender, silent
from app.topics import TopicService, migrate_json_topics

log = logging.getLogger("max2tg")

# Outages shorter than this are not worth warning about: MAX usually replays
# what it buffered, and a notice on every blip is a notice nobody reads. Longer
# than this and the owner has to be told, because the bridge has no catch-up of
# its own — there is no confirmed history/sync mechanism in the protocol to ask
# for what it missed (audit R07).
GAP_NOTICE_SEC = 120.0


def setup_logging(settings: Settings) -> None:
    """Configure logging, with redaction installed before the first record."""
    logging_utils.register_secrets([settings.max_token, settings.max_device_id,
                                    settings.tg_bot_token])
    logging_utils.set_policy(logging_utils.TextPolicy(settings.log_message_text))

    level = logging.DEBUG if settings.debug else getattr(
        logging, settings.log_level, logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    file_handler = RotatingFileHandler(
        filename=os.path.join(settings.log_dir, "max2tg.log"),
        maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(fmt)

    handlers = [console, file_handler]
    logging_utils.install(handlers)
    logging.basicConfig(level=level, handlers=handlers, force=True)

    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(
        logging.DEBUG if settings.debug else logging.WARNING)


async def run_bridge(settings: Settings) -> int:
    health = Health()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    # A real pool. The previous build installed an executor that ran everything
    # inline, which meant every DNS lookup froze both transports, the heartbeat
    # and all timers for as long as the resolver took (audit R09).
    loop.set_default_executor(ThreadPoolExecutor(
        max_workers=settings.executor_threads, thread_name_prefix="io"))

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            log.debug("Signal %s cannot be handled on this platform", sig)

    async with AsyncExitStack() as stack:
        store = Store(settings.db_path, synchronous=settings.sqlite_synchronous,
                      retention_days=settings.retention_days)
        await store.open()
        stack.push_async_callback(store.close)
        await store.check_manifest(account="", group_id=str(settings.tg_chat_id))

        imported = await migrate_json_topics(store, settings.legacy_topics_path)
        if imported:
            log.info("Migrated %d topic mapping(s) from topics.json", imported)

        spool = MediaSpool(settings.spool_dir, settings.spool_budget_bytes)
        session_holder: dict[str, aiohttp.ClientSession] = {}

        policy = FetchPolicy(
            allowed_host_suffixes=tuple(settings.media_allowed_hosts),
            allow_any_host=settings.media_allow_any_host,
            allow_plain_http=settings.media_allow_plain_http,
            trusted_nets=tuple(settings.media_trusted_nets),
            max_file_bytes=settings.max_media_bytes,
        )
        media_session = aiohttp.ClientSession(headers=_BROWSER_HEADERS)
        stack.push_async_callback(media_session.close)
        session_holder["media"] = media_session
        fetcher = SafeFetcher(lambda: session_holder["media"], policy, spool,
                              headers=_HTTP_HEADERS,
                              concurrency=settings.download_concurrency)

        sender = TelegramSender(settings.tg_bot_token, str(settings.tg_chat_id),
                                proxy_url=settings.tg_proxy,
                                status_chat_id=(str(settings.tg_status_chat_id)
                                                if settings.tg_status_chat_id else None))
        await sender.start()
        stack.push_async_callback(sender.stop)

        for problem in await sender.preflight():
            log.error("Telegram preflight: %s", problem)

        notifier = StatusNotifier(sender, settings.tg_status_chat_id)
        notifier.start()
        stack.push_async_callback(notifier.stop)

        client = MaxClient(
            settings.max_token, settings.max_device_id,
            chat_ids=",".join(settings.inbound_chat_ids) or None,
            debug=settings.debug, health=health, fetcher=fetcher, spool=spool,
            enable_audio_probing=settings.enable_audio_probing)
        resolver = ContactResolver(client=client)
        client.resolver = resolver  # /profile and friends reach it through here
        topics = TopicService(store, sender.bot, settings.tg_chat_id)

        orphans = await topics.report_orphans()
        for orphan in orphans:
            log.warning("Topic creation for %s was started and never finished — "
                        "either it failed outright, or there is an orphan topic "
                        "in the supergroup that nothing routes to", orphan)

        account = settings.max_device_id
        retry = RetryPolicy(max_attempts=settings.max_attempts,
                            base_delay=settings.retry_base_delay)

        inbound_handler = MaxToTelegramHandler(
            client, sender, topics, resolver, health,
            max_media_bytes=settings.max_media_bytes)
        inbound = DeliveryPipeline(store, source=MAX_SOURCE, account=account,
                                   handler=inbound_handler, health=health,
                                   retry=retry,
                                   on_dead_letter=_dead_letter(sender, topics))
        inbound_handler.bind(inbound)
        await inbound.start()
        stack.push_async_callback(inbound.stop, settings.shutdown_timeout)

        outbound_handler = TelegramToMaxHandler(
            client, sender.bot, spool, health,
            reply_link=settings.max_reply_link)
        outbound = DeliveryPipeline(store, source=TG_SOURCE, account=account,
                                    handler=outbound_handler, health=health,
                                    retry=retry)
        outbound_handler.bind(outbound)
        await outbound.start()
        stack.push_async_callback(outbound.stop, settings.shutdown_timeout)

        _wire_client(client, resolver, inbound, notifier, store, settings, health)

        tg_app = None
        if settings.reply_enabled:
            bridge = Bridge(
                client=client, resolver=resolver, topics=topics, health=health,
                supergroup_id=settings.tg_chat_id,
                allowed_user_ids=settings.tg_allowed_user_ids,
                admin_user_ids=settings.tg_admin_user_ids,
                allow_any_member=settings.allow_any_member,
                max_pipeline=inbound, tg_pipeline=outbound)
            tg_app = build_tg_app(settings.tg_bot_token, bridge,
                                  proxy_url=settings.tg_proxy)
            await tg_app.initialize()
            await tg_app.start()
            await tg_app.updater.start_polling(
                # Off by default now: dropping them silently discards everything
                # typed while the bridge was down (audit S01).
                drop_pending_updates=settings.drop_pending_updates,
                allowed_updates=Update.ALL_TYPES,
            )
            stack.push_async_callback(_stop_telegram, tg_app)
            log.info("Telegram polling started (replies to MAX enabled)")
        else:
            log.info("REPLY_ENABLED=false — Telegram ingress is off")

        link = asyncio.create_task(client.run(), name="max-link")
        heartbeat = asyncio.create_task(
            _publish_health(health, inbound, settings), name="health-writer")
        log.info("Bridge is up. Waiting for MAX...")

        await _wait_for_stop(stop, link)
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)

        log.info("Shutting down...")
        health.set_state(LinkState.STOPPING)
        await client.stop()
        link.cancel()
        await asyncio.gather(link, return_exceptions=True)

    log.info("Stopped cleanly.")
    return 0


async def _publish_health(health: Health, inbound: DeliveryPipeline,
                          settings: Settings, interval: float = 15.0) -> None:
    """Keep the on-disk snapshot fresh for the container health check.

    Writing it from inside the loop is the point: a snapshot that stops being
    updated is itself the signal that the loop is wedged. A state transition
    publishes straight away, so a probe does not report stale readiness for a
    whole interval after the link comes back.
    """
    path = health_path(settings.state_dir)
    while True:
        # Cleared *before* the snapshot is taken: a transition that happens
        # while we are writing then re-arms the event and wakes us on the next
        # loop, instead of being swallowed by a clear that came after it.
        health.changed.clear()
        try:
            snapshot = health.snapshot()
            stats = await inbound.stats()
            snapshot["oldest_pending_age"] = stats.get("oldest_pending_age")
            snapshot["pending"] = stats.get("pending")
            snapshot["failed"] = stats.get("failed")
            write_snapshot(path, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.debug("Could not publish the health snapshot", exc_info=True)
        try:
            await asyncio.wait_for(health.changed.wait(), timeout=interval)
        except TimeoutError:
            pass


async def _wait_for_stop(stop: asyncio.Event, link: asyncio.Task) -> None:
    waiter = asyncio.create_task(stop.wait(), name="stop-signal")
    done, _pending = await asyncio.wait({waiter, link},
                                        return_when=asyncio.FIRST_COMPLETED)
    waiter.cancel()
    if link in done and not link.cancelled():
        exc = link.exception()
        if exc is not None:
            log.error("MAX link task ended: %s", describe_exception(exc))


async def _stop_telegram(tg_app) -> None:
    try:
        if tg_app.updater and tg_app.updater.running:
            await tg_app.updater.stop()
        await tg_app.stop()
    finally:
        await tg_app.shutdown()


def _dead_letter(sender: TelegramSender, topics: TopicService):
    """Tell the owner, in the topic, that a message was given up on.

    A gap in a conversation that nobody is told about is worse than a visible
    "this did not arrive" line.
    """
    async def notify(job, reason: str) -> None:
        # A service note in a shared group: it must not ring every member's
        # phone. The owner's alert channel is the status chat.
        with silent():
            await _post(job, reason)

    async def _post(job, reason: str) -> None:
        thread_id = await topics.get_thread(job.chat_key)
        if thread_id is not None:
            await sender.send_text(
                f"⚠️ <i>Сообщение из MAX не доставлено: {reason}. "
                f"Повторить — <code>/retry here</code>.</i>",
                thread_id)
            return
        # No topic is usually *why* it failed. General is the only place left to
        # say so, and the notice has to name the chat to be of any use there.
        await sender.send_text(
            f"⚠️ <i>Сообщение из MAX-чата <code>{job.chat_key}</code> не "
            f"доставлено: {reason}. Топик для этого чата создать не удалось. "
            f"Повторить — <code>/retry</code>.</i>",
            None)
    return notify


def _wire_client(client: MaxClient, resolver: ContactResolver,
                 inbound: DeliveryPipeline, notifier: StatusNotifier,
                 store: Store, settings: Settings, health: Health) -> None:
    first_ready = {"value": True}

    @client.on_ready
    async def handle_ready(snapshot: dict) -> None:
        participants = resolver.load_snapshot(snapshot)
        resolver.forget_transient_failures()
        if participants:
            await resolver.resolve_users_batch(participants)

        if client.my_id is not None:
            try:
                await store.check_manifest(account=str(client.my_id),
                                           group_id=str(settings.tg_chat_id))
            except StoreCorrupted as exc:
                log.error("State does not match this account: %s", exc)

        if first_ready["value"]:
            notifier.notify(f"✅ <b>MAX:</b> подключён | чатов: "
                            f"{len(resolver.chats)}")
            first_ready["value"] = False
        else:
            notifier.resolve_incident("✅ <b>MAX:</b> связь и авторизация "
                                      "восстановлены")

        outage = health.last_outage_sec
        if outage and outage >= GAP_NOTICE_SEC:
            # Deliberately not routed through the incident throttle: "you may
            # have lost messages" is the one notice that must never be
            # suppressed because an earlier one was recent.
            notifier.notify(
                f"⚠️ <b>Пропуск:</b> мост был без связи с MAX "
                f"{format_duration(outage)}. Сообщения за это время дойдут, "
                f"только если MAX сам их повторит — своей докачки истории у "
                f"моста нет, подтверждённого history/sync в протоколе не "
                f"найдено. Проверь чаты за этот период вручную.")
            health.last_outage_sec = None

    @client.on_disconnect
    async def handle_disconnect(reason: str) -> None:
        if health.state is LinkState.AUTH_REQUIRED:
            notifier.notify_incident(
                "⛔ <b>MAX отклонил токен.</b> Нужен новый MAX_TOKEN — "
                "переподключение само не поможет.")
            return
        notifier.notify_incident(
            f"⚠️ <b>MAX:</b> связь потеряна ({_short(reason)}), переподключаюсь…")

    @client.on_message
    async def handle_message(msg) -> None:
        # Fast and durable: the journal write is the only thing the read loop
        # waits for, so RPC replies keep flowing while deliveries happen
        # elsewhere.
        await inbound.submit(chat=msg.chat_id, event_id=msg.message_id,
                             revision=msg.revision, payload=msg.to_payload())

    @client.on_quarantine
    async def handle_quarantine(reason: str, payload: dict) -> None:
        log.warning("Quarantined a MAX event: %s", reason)


def _short(text: str, limit: int = 120) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[:limit - 1] + "…"


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    setup_logging(settings)
    log_notes(settings, log)
    log.info("max2tg starting | log level %s | state %s",
             settings.log_level, settings.state_dir)

    try:
        return asyncio.run(run_bridge(settings))
    except KeyboardInterrupt:
        log.info("Interrupted.")
        return 0
    except StoreCorrupted as exc:
        log.error("Refusing to start: %s", exc)
        return 3
    except Exception as exc:
        log.critical("Fatal: %s", describe_exception(exc), exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
