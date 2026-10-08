"""Outbound HTTP with a policy: where we may go, and how much we may read.

Two separate problems live here.

*Where.* Attachment and avatar URLs arrive inside protocol objects. The old code
handed them straight to aiohttp after a ``startswith("http")`` check, following
redirects automatically. If anything upstream can influence those fields, that
is an SSRF primitive whose output gets posted into Telegram (audit S05). Every
hop is now checked: scheme, host against an allowlist, and the resolved
addresses against the private/loopback/link-local ranges.

*How much.* Downloads were read whole into memory with ``resp.read()`` and then
parked in an album buffer while other chats downloaded in parallel — an
unbounded appetite that OOM-kills the process and takes every queue with it
(audit M08). Bodies now stream to a spool file under a per-file cap and a
process-wide disk quota, and the cap is enforced against bytes actually read,
not against a ``Content-Length`` the server is free to lie about.

Residual risk worth naming: validating resolved addresses and then letting
aiohttp resolve the name again leaves a DNS-rebinding window. Closing it fully
means pinning the connection to the validated IP with the original SNI. For a
personal bridge talking to one CDN the allowlist is the real control, and the IP
check is defence in depth.

Fake-IP DNS (a router running a TUN proxy such as mihomo/sing-box) answers every
lookup with an address from 198.18.0.0/15 and resolves the real destination
itself. To the range check that looks like a private address, so every media
download fails. ``FetchPolicy.trusted_nets`` lets the operator name such a range
explicitly; the host allowlist still applies to every hop.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import socket
import tempfile
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from app.errors import FileTooLarge, MediaTooLarge, UnsafeUrl

log = logging.getLogger(__name__)

# Hosts MAX actually serves media and uploads from. Anything else has to be
# opted into explicitly, because an allowlist that grows by accident is not one.
DEFAULT_ALLOWED_HOST_SUFFIXES = ("oneme.ru", "max.ru", "mycdn.me")

_CHUNK = 64 * 1024

# RFC 2544 benchmarking range — what fake-IP DNS resolvers hand out.
FAKE_IP_NET = ipaddress.ip_network("198.18.0.0/15")

IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


@dataclass
class FetchPolicy:
    allowed_host_suffixes: tuple[str, ...] = DEFAULT_ALLOWED_HOST_SUFFIXES
    allow_any_host: bool = False
    allow_plain_http: bool = False
    trusted_nets: tuple[IPNetwork, ...] = ()
    max_file_bytes: int = 64 * 1024 * 1024
    max_redirects: int = 3
    connect_timeout: float = 15.0
    read_timeout: float = 180.0

    def host_allowed(self, host: str) -> bool:
        if self.allow_any_host:
            return True
        host = (host or "").lower().rstrip(".")
        return any(host == suffix or host.endswith("." + suffix)
                   for suffix in self.allowed_host_suffixes)

    def address_allowed(self, addr: str) -> bool:
        if is_public_ip(addr):
            return True
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            return False
        return any(ip in net for net in self.trusted_nets)


def is_public_ip(addr: str) -> bool:
    """False for anything that could reach infrastructure we host ourselves."""
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        return False
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
        or ip.is_reserved or ip.is_unspecified
    )


async def validate_url(url: str, policy: FetchPolicy) -> str:
    """Check one URL against the policy and return its host.

    Raises :class:`UnsafeUrl` with a reason the log can carry without leaking
    the signed query string that usually rides on these URLs.
    """
    if not isinstance(url, str) or not url:
        raise UnsafeUrl("empty URL")
    parts = urlsplit(url)
    scheme = (parts.scheme or "").lower()
    if scheme == "http" and not policy.allow_plain_http:
        raise UnsafeUrl("plain HTTP is not allowed")
    if scheme not in ("http", "https"):
        raise UnsafeUrl(f"unsupported scheme {scheme!r}")
    host = parts.hostname
    if not host:
        raise UnsafeUrl("URL has no host")
    if not policy.host_allowed(host):
        raise UnsafeUrl(f"host {host!r} is not in the media allowlist")

    # A literal IP needs no lookup, but still needs the range check.
    try:
        ipaddress.ip_address(host)
        addresses = [host]
    except ValueError:
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, parts.port or (443 if scheme == "https" else 80),
                                           type=socket.SOCK_STREAM)
        except OSError as exc:
            raise UnsafeUrl(f"cannot resolve {host!r}: {exc}") from exc
        addresses = [info[4][0] for info in infos]
    if not addresses:
        raise UnsafeUrl(f"{host!r} resolved to nothing")
    for addr in addresses:
        if not policy.address_allowed(addr):
            hint = ""
            try:
                if ipaddress.ip_address(addr) in FAKE_IP_NET:
                    hint = (" (looks like fake-IP DNS from a proxy; set "
                            "MEDIA_TRUSTED_NETS=198.18.0.0/15 if that is expected)")
            except ValueError:
                pass
            raise UnsafeUrl(f"{host!r} resolves to non-public address{hint}")
    return host


class SpoolFile:
    """A downloaded body on disk, with its quota accounted for until released."""

    def __init__(self, path: str, filename: str, content_type: str,
                 size: int, spool: MediaSpool):
        self.path = path
        self.filename = filename
        self.content_type = content_type
        self.size = size
        self._spool = spool
        self._released = False

    def open(self):
        """Fresh handle per call — a retry cannot re-read a consumed stream."""
        return open(self.path, "rb")

    def read_bytes(self) -> bytes:
        with self.open() as fh:
            return fh.read()

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._spool._release(self.path, self.size)

    def __enter__(self) -> SpoolFile:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


class MediaSpool:
    """Disk area for in-flight media, with a hard total budget."""

    def __init__(self, directory: str, max_total_bytes: int = 512 * 1024 * 1024):
        self.directory = directory
        self.max_total_bytes = max_total_bytes
        self._used = 0
        self._lock = asyncio.Lock()
        os.makedirs(directory, exist_ok=True)
        self._cleanup_orphans()

    def _cleanup_orphans(self) -> None:
        """Remove spool files left behind by a previous crash."""
        try:
            for name in os.listdir(self.directory):
                if name.startswith("media-"):
                    try:
                        os.unlink(os.path.join(self.directory, name))
                    except OSError:
                        pass
        except OSError:
            log.warning("Cannot list spool directory %s", self.directory)

    @property
    def used_bytes(self) -> int:
        return self._used

    async def reserve(self, size: int) -> None:
        async with self._lock:
            if self._used + size > self.max_total_bytes:
                raise MediaTooLarge(
                    f"media spool budget exhausted "
                    f"({self._used + size} > {self.max_total_bytes} bytes)"
                )
            self._used += size

    def _release(self, path: str, size: int) -> None:
        self._used = max(0, self._used - size)
        try:
            os.unlink(path)
        except OSError:
            pass

    def new_temp(self) -> tuple[int, str]:
        return tempfile.mkstemp(prefix="media-", dir=self.directory)


class SafeFetcher:
    """Policy-checked, size-bounded HTTP client for protocol-supplied URLs."""

    def __init__(self, session_factory, policy: FetchPolicy, spool: MediaSpool,
                 headers: dict | None = None,
                 concurrency: int = 4):
        self._session_factory = session_factory
        self._policy = policy
        self._spool = spool
        self._headers = headers or {}
        self._sem = asyncio.Semaphore(max(1, concurrency))

    @property
    def policy(self) -> FetchPolicy:
        return self._policy

    async def download(self, url: str, *, filename: str = "file",
                       max_bytes: int | None = None) -> SpoolFile:
        """Stream a URL to the spool. Raises on policy, size or transport errors."""
        limit = max_bytes or self._policy.max_file_bytes
        async with self._sem:
            return await self._download_inner(url, filename, limit)

    async def _download_inner(self, url: str, filename: str,
                              limit: int) -> SpoolFile:
        current = url
        for _hop in range(self._policy.max_redirects + 1):
            await validate_url(current, self._policy)
            session = self._session_factory()
            timeout = aiohttp.ClientTimeout(
                sock_connect=self._policy.connect_timeout,
                total=self._policy.read_timeout,
            )
            async with session.get(current, headers=self._headers,
                                   timeout=timeout,
                                   allow_redirects=False) as resp:
                if resp.status in (301, 302, 303, 307, 308):
                    location = resp.headers.get("Location")
                    if not location:
                        raise UnsafeUrl("redirect without Location")
                    current = str(resp.url.join(aiohttp.client.URL(location)))
                    continue
                if resp.status != 200:
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history,
                        status=resp.status,
                        message=f"HTTP {resp.status}",
                    )
                declared = resp.headers.get("Content-Length")
                if declared and declared.isdigit() and int(declared) > limit:
                    raise FileTooLarge(
                        f"declared size {int(declared)} exceeds limit {limit}",
                        size=int(declared))
                return await self._spool_body(resp, filename, limit)
        raise UnsafeUrl(f"too many redirects (>{self._policy.max_redirects})")

    async def _spool_body(self, resp: aiohttp.ClientResponse, filename: str,
                          limit: int) -> SpoolFile:
        fd, path = self._spool.new_temp()
        written = 0
        reserved = 0
        try:
            with os.fdopen(fd, "wb") as out:
                async for chunk in resp.content.iter_chunked(_CHUNK):
                    written += len(chunk)
                    if written > limit:
                        raise FileTooLarge(
                            f"body exceeded {limit} bytes while streaming")
                    # Reserve as we go: a lying Content-Length cannot get us to
                    # overshoot the process-wide budget.
                    if written > reserved:
                        need = written - reserved
                        await self._spool.reserve(need)
                        reserved += need
                    out.write(chunk)
        except BaseException:
            if reserved:
                self._spool._release(path, reserved)
            else:
                try:
                    os.unlink(path)
                except OSError:
                    pass
            raise
        content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
        return SpoolFile(path, filename, content_type, written, self._spool)


@dataclass
class UploadLimits:
    """Byte ceilings for the two directions, which are genuinely different.

    A bot downloading an attachment through ``getFile`` is capped at 20 MB by
    the Bot API; what a bot may *send* is far larger. Collapsing both into one
    "20 MB limit" is the documentation error the audit called out (M08/O06).
    """

    telegram_download_bytes: int = 20 * 1024 * 1024
    telegram_upload_bytes: int = 50 * 1024 * 1024
    max_download_bytes: int = 64 * 1024 * 1024


def sniff_media_kind(filename: str, content_type: str, head: bytes) -> str:
    """Classify media by magic bytes first, extension second.

    Extension-only classification sent ``.avi``/``.mkv`` to ``sendVideo``, which
    Telegram rejects, and the request was then retried three times with the same
    doomed payload (audit M09).
    """
    ct = (content_type or "").lower()
    if head[:3] == b"\xff\xd8\xff" or head[:8] == b"\x89PNG\r\n\x1a\n":
        return "photo"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "animation"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "photo"
    if head[4:8] == b"ftyp":
        return "video"
    if head[:4] == b"\x1aE\xdf\xa3":  # Matroska / WebM
        return "video"
    if ct.startswith("image/gif"):
        return "animation"
    if ct.startswith("image/"):
        return "photo"
    if ct.startswith("video/mp4") or ct.startswith("video/quicktime"):
        return "video"
    if ct.startswith("audio/"):
        return "audio"

    name = (filename or "").lower()
    if name.endswith((".jpg", ".jpeg", ".png", ".webp")):
        return "photo"
    if name.endswith(".gif"):
        return "animation"
    if name.endswith((".mp4", ".mov")):
        return "video"
    if name.endswith((".ogg", ".oga", ".opus", ".mp3", ".m4a")):
        return "audio"
    # .avi, .mkv, .bmp and friends deliberately fall through to document: they
    # are not what Telegram's photo/video endpoints accept.
    return "document"


def looks_like_media(content_type: str) -> bool:
    """Reject the HTML error pages and JSON blobs a probe would otherwise accept."""
    ct = (content_type or "").lower()
    if not ct:
        return False
    if ct.startswith(("text/", "application/json", "application/xml")):
        return False
    return ct.startswith(("image/", "video/", "audio/", "application/octet-stream"))


@dataclass
class HostPolicyReport:
    """What the fetch policy is configured to allow — surfaced by ``/status``."""

    allow_any_host: bool
    suffixes: tuple[str, ...] = field(default_factory=tuple)

    def describe(self) -> str:
        if self.allow_any_host:
            return "any host (MEDIA_ALLOW_ANY_HOST=true)"
        return ", ".join(self.suffixes) or "(nothing allowed)"
