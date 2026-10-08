"""Settings: parsed, normalised and validated before anything opens a socket.

``MAX_CHAT_IDS`` used to be parsed inside the MAX client, well after the
Telegram sender had started, so a typo there raised an unhandled ``ValueError``
past the startup path and left clients half-open (audit O05). Everything is
checked here, and a bad value produces a sentence saying what to fix.

The one behavioural change worth flagging: the bridge no longer starts in
"anybody in the supergroup may send messages as my MAX account" mode by
accident. Either name the people who may (``TG_ALLOWED_USER_ID``) or say
explicitly that the whole group may (``TG_ALLOW_ANY_MEMBER=true``) — audit S03.
"""

from __future__ import annotations

import ipaddress
import logging
import os
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from dotenv import load_dotenv

from app.net import DEFAULT_ALLOWED_HOST_SUFFIXES

_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off", "")

_VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class ConfigError(SystemExit):
    """Raised with a message the operator can act on."""


def _get(name: str) -> str:
    """Environment value with surrounding whitespace removed.

    A variable set to spaces used to count as present and then failed somewhere
    far away.
    """
    return (os.environ.get(name) or "").strip()


def _flag(name: str, default: bool = False) -> bool:
    raw = _get(name).lower()
    if not raw:
        return default
    if raw in _TRUE:
        return True
    if raw in _FALSE:
        return False
    raise ConfigError(f"{name} must be true or false, got {raw!r}")


def _int(name: str, default: int | None = None, *, positive: bool = False
         ) -> int | None:
    raw = _get(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None
    if positive and value <= 0:
        raise ConfigError(f"{name} must be a positive integer, got {value}")
    return value


def _float(name: str, default: float) -> float:
    raw = _get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from None


def _id_set(name: str) -> frozenset[int]:
    """Comma-separated Telegram user IDs; ``0`` is not a user."""
    raw = _get(name)
    if not raw:
        return frozenset()
    ids: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            value = int(chunk)
        except ValueError:
            raise ConfigError(
                f"{name} must be comma-separated user IDs, got {chunk!r}") from None
        if value <= 0:
            raise ConfigError(
                f"{name} contains {value}, which is not a valid Telegram user ID. "
                "Leave the variable unset if you meant 'no restriction'.")
        ids.add(value)
    return frozenset(ids)


def _chat_ids(name: str) -> tuple[str, ...]:
    raw = _get(name)
    if not raw:
        return ()
    result: list[str] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            result.append(str(int(chunk)))
        except ValueError:
            raise ConfigError(
                f"{name} must be comma-separated MAX chat IDs, got {chunk!r}"
            ) from None
    return tuple(result)


def _writable_dir(name: str, path: str) -> str:
    """Create the directory now, so a permission problem surfaces at startup."""
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"{name}={path!r} cannot be created: {exc}") from None
    if not os.access(path, os.W_OK | os.X_OK):
        raise ConfigError(f"{name}={path!r} is not writable by this process")
    return path


@dataclass(frozen=True)
class Settings:
    # credentials
    max_token: str
    max_device_id: str
    tg_bot_token: str
    tg_chat_id: int

    # routing / access
    inbound_chat_ids: tuple[str, ...] = ()
    tg_allowed_user_ids: frozenset[int] = frozenset()
    tg_admin_user_ids: frozenset[int] = frozenset()
    allow_any_member: bool = False
    tg_status_chat_id: int | None = None
    reply_enabled: bool = True
    drop_pending_updates: bool = False

    # transport
    tg_proxy: str | None = None

    # storage
    state_dir: str = "state"
    spool_dir: str = "state/spool"
    log_dir: str = "logs"
    sqlite_synchronous: str = "FULL"
    retention_days: float = 14.0

    # delivery
    max_attempts: int = 5
    retry_base_delay: float = 5.0
    max_media_bytes: int = 48 * 1024 * 1024
    spool_budget_bytes: int = 512 * 1024 * 1024
    download_concurrency: int = 4

    # media fetch policy
    media_allowed_hosts: tuple[str, ...] = DEFAULT_ALLOWED_HOST_SUFFIXES
    media_allow_any_host: bool = False
    media_allow_plain_http: bool = False
    media_trusted_nets: tuple = ()

    # diagnostics
    debug: bool = False
    log_level: str = "INFO"
    log_message_text: bool = False
    enable_audio_probing: bool = False
    max_reply_link: bool = False

    # tuning
    executor_threads: int = 4
    shutdown_timeout: float = 25.0
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def db_path(self) -> str:
        return os.path.join(self.state_dir, "bridge.sqlite3")

    @property
    def legacy_topics_path(self) -> str:
        return os.path.join(self.state_dir, "topics.json")


def load_settings() -> Settings:
    load_dotenv()
    notes: list[str] = []

    required = ["MAX_TOKEN", "MAX_DEVICE_ID", "TG_BOT_TOKEN", "TG_CHAT_ID"]
    missing = [name for name in required if not _get(name)]
    if missing:
        raise ConfigError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Copy .env.example to .env and fill them in.")

    tg_chat_id = _int("TG_CHAT_ID")
    if tg_chat_id is None:
        raise ConfigError("TG_CHAT_ID must be an integer")
    if tg_chat_id >= 0:
        notes.append(
            f"TG_CHAT_ID={tg_chat_id} is not negative; a supergroup ID normally "
            "looks like -1001234567890. Check you copied the right one.")

    allowed = _id_set("TG_ALLOWED_USER_ID")
    admins = _id_set("TG_ADMIN_USER_ID")
    allow_any = _flag("TG_ALLOW_ANY_MEMBER", False)
    if not allowed and not allow_any:
        raise ConfigError(
            "Nobody is allowed to send messages as your MAX account.\n"
            "Set TG_ALLOWED_USER_ID to your Telegram user ID (ask @userinfobot), "
            "or set TG_ALLOW_ANY_MEMBER=true if every member of the supergroup "
            "really should be able to write as you and manage topics.")
    if allow_any and not allowed:
        notes.append(
            "TG_ALLOW_ANY_MEMBER=true: every member of the supergroup can send "
            "messages as your MAX account and manage topics.")
    if admins and not admins <= allowed and allowed:
        notes.append("TG_ADMIN_USER_ID lists IDs outside TG_ALLOWED_USER_ID; "
                     "they may manage topics but not send messages.")

    log_level = (_get("LOG_LEVEL") or "INFO").upper()
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(
            f"LOG_LEVEL must be one of {', '.join(_VALID_LOG_LEVELS)}, "
            f"got {log_level!r}")

    proxy = _get("TG_PROXY") or None
    if proxy:
        parsed = urlsplit(proxy)
        if parsed.scheme not in ("socks5", "socks5h", "http", "https") or not parsed.hostname:
            raise ConfigError(
                "TG_PROXY must look like socks5://host:port or http://host:port")

    state_dir = _writable_dir("STATE_DIR", _get("STATE_DIR") or "state")
    log_dir = _writable_dir("LOG_DIR", _get("LOG_DIR") or "logs")
    spool_dir = _writable_dir("SPOOL_DIR",
                              _get("SPOOL_DIR") or os.path.join(state_dir, "spool"))

    synchronous = (_get("SQLITE_SYNCHRONOUS") or "FULL").upper()
    if synchronous not in ("OFF", "NORMAL", "FULL", "EXTRA"):
        raise ConfigError("SQLITE_SYNCHRONOUS must be OFF, NORMAL, FULL or EXTRA")
    if synchronous in ("OFF", "NORMAL"):
        notes.append(
            f"SQLITE_SYNCHRONOUS={synchronous}: faster, but a power loss can "
            "lose the most recent delivery records.")

    hosts_raw = _get("MEDIA_ALLOWED_HOSTS")
    hosts = (tuple(h.strip().lower() for h in hosts_raw.split(",") if h.strip())
             if hosts_raw else DEFAULT_ALLOWED_HOST_SUFFIXES)
    allow_any_host = _flag("MEDIA_ALLOW_ANY_HOST", False)
    if allow_any_host:
        notes.append(
            "MEDIA_ALLOW_ANY_HOST=true: the bridge will fetch attachment URLs "
            "from any host. Private and loopback addresses are still refused.")

    trusted_nets = _trusted_nets(_get("MEDIA_TRUSTED_NETS"))
    if trusted_nets:
        notes.append(
            "MEDIA_TRUSTED_NETS=" + ",".join(str(n) for n in trusted_nets)
            + ": media hosts may resolve into these ranges. Use this only for "
            "fake-IP DNS from a proxy you control.")

    log_text = _flag("LOG_MESSAGE_TEXT", False)
    if log_text:
        notes.append(
            "LOG_MESSAGE_TEXT=true: message bodies and contact names will be "
            "written to the log file. Restrict who can read logs/.")

    debug = _flag("DEBUG", False)
    if debug:
        notes.append(
            "DEBUG=true: raw protocol payloads are dumped to debug/. They "
            "contain personal data; delete them when you are done.")

    reply_link = _flag("MAX_REPLY_LINK", False)
    if reply_link:
        notes.append(
            "MAX_REPLY_LINK=true: replies from Telegram are sent to MAX with a "
            "link field whose wire format has never been confirmed. If MAX "
            "rejects it the socket may close, so watch for reconnects and turn "
            "this back off if you see them.")

    drop_pending = _flag("TG_DROP_PENDING_UPDATES", False)
    if drop_pending:
        notes.append(
            "TG_DROP_PENDING_UPDATES=true: messages you sent in Telegram while "
            "the bridge was down will be discarded rather than delivered.")

    return Settings(
        max_token=_get("MAX_TOKEN"),
        max_device_id=_get("MAX_DEVICE_ID"),
        tg_bot_token=_get("TG_BOT_TOKEN"),
        tg_chat_id=tg_chat_id,
        inbound_chat_ids=_chat_ids("MAX_CHAT_IDS"),
        tg_allowed_user_ids=allowed,
        tg_admin_user_ids=admins,
        allow_any_member=allow_any,
        tg_status_chat_id=_int("TG_STATUS_CHAT_ID"),
        reply_enabled=_flag("REPLY_ENABLED", True),
        drop_pending_updates=drop_pending,
        tg_proxy=proxy,
        state_dir=state_dir,
        spool_dir=spool_dir,
        log_dir=log_dir,
        sqlite_synchronous=synchronous,
        retention_days=_float("JOURNAL_RETENTION_DAYS", 14.0),
        max_attempts=_int("DELIVERY_MAX_ATTEMPTS", 5, positive=True),
        retry_base_delay=_float("DELIVERY_RETRY_BASE_SEC", 5.0),
        max_media_bytes=_int("MEDIA_MAX_BYTES", 48 * 1024 * 1024, positive=True),
        spool_budget_bytes=_int("SPOOL_BUDGET_BYTES", 512 * 1024 * 1024,
                                positive=True),
        download_concurrency=_int("DOWNLOAD_CONCURRENCY", 4, positive=True),
        media_allowed_hosts=hosts,
        media_allow_any_host=allow_any_host,
        media_allow_plain_http=_flag("MEDIA_ALLOW_PLAIN_HTTP", False),
        media_trusted_nets=trusted_nets,
        debug=debug,
        log_level=log_level,
        log_message_text=log_text,
        enable_audio_probing=_flag("MAX_AUDIO_PROBING", False),
        max_reply_link=reply_link,
        executor_threads=_int("EXECUTOR_THREADS", 4, positive=True),
        shutdown_timeout=_float("SHUTDOWN_TIMEOUT_SEC", 25.0),
        notes=tuple(notes),
    )


def _trusted_nets(raw: str | None) -> tuple:
    nets = []
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            net = ipaddress.ip_network(item, strict=False)
        except ValueError:
            raise ConfigError(
                f"MEDIA_TRUSTED_NETS: {item!r} is not a CIDR range") from None
        # Trusting these would turn the media fetcher into a way to reach the
        # host itself, which is exactly what the range check exists to stop.
        if (net.overlaps(ipaddress.ip_network("127.0.0.0/8"))
                if net.version == 4 else
                net.overlaps(ipaddress.ip_network("::1/128"))) \
                or net.is_link_local or net.prefixlen == 0:
            raise ConfigError(
                f"MEDIA_TRUSTED_NETS: {item!r} covers loopback, link-local or "
                "everything — refusing")
        nets.append(net)
    return tuple(nets)


def log_notes(settings: Settings, logger: logging.Logger) -> None:
    for note in settings.notes:
        logger.warning("Configuration note: %s", note)
