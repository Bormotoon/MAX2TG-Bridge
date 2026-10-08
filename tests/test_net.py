"""Fetch policy: where we may go, how much we may read, what we think it is."""

from __future__ import annotations

import ipaddress
import os
import socket

import pytest

from app.errors import MediaTooLarge, UnsafeUrl
from app.net import (
    DEFAULT_ALLOWED_HOST_SUFFIXES,
    FetchPolicy,
    MediaSpool,
    SpoolFile,
    is_public_ip,
    looks_like_media,
    sniff_media_kind,
    validate_url,
)


@pytest.fixture
def policy():
    return FetchPolicy()


def _fake_dns(monkeypatch, addr: str) -> None:
    """Make every lookup answer ``addr``. Tests must not depend on the resolver
    of the machine they run on: a fake-IP router once broke them."""
    async def getaddrinfo(self, host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (addr, port))]
    monkeypatch.setattr("asyncio.base_events.BaseEventLoop.getaddrinfo",
                        getaddrinfo)


@pytest.fixture
def public_dns(monkeypatch):
    _fake_dns(monkeypatch, "93.186.225.10")


class TestIsPublicIp:
    @pytest.mark.parametrize("addr", [
        "127.0.0.1", "10.0.0.5", "192.168.1.1", "172.16.0.1", "169.254.169.254",
        "::1", "fe80::1", "0.0.0.0", "224.0.0.1",
    ])
    def test_private_and_special_are_refused(self, addr):
        assert is_public_ip(addr) is False

    @pytest.mark.parametrize("addr", ["8.8.8.8", "1.1.1.1", "2001:4860:4860::8888"])
    def test_public_is_allowed(self, addr):
        assert is_public_ip(addr) is True

    def test_garbage_is_refused(self):
        assert is_public_ip("not-an-ip") is False


class TestHostAllowlist:
    def test_exact_host_matches(self, policy):
        assert policy.host_allowed("max.ru") is True

    def test_subdomain_matches(self, policy):
        assert policy.host_allowed("i.oneme.ru") is True

    def test_lookalike_suffix_is_refused(self, policy):
        """``notmax.ru`` must not pass as ``max.ru``."""
        assert policy.host_allowed("notmax.ru") is False

    def test_unrelated_host_is_refused(self, policy):
        assert policy.host_allowed("evil.example") is False

    def test_case_and_trailing_dot_are_normalised(self, policy):
        assert policy.host_allowed("I.ONEME.RU.") is True

    def test_allow_any_host_opens_it_up(self):
        assert FetchPolicy(allow_any_host=True).host_allowed("evil.example") is True

    def test_defaults_are_the_max_cdns(self):
        assert "oneme.ru" in DEFAULT_ALLOWED_HOST_SUFFIXES


class TestValidateUrl:
    async def test_allowed_https_url_passes(self, policy, public_dns):
        assert await validate_url("https://i.oneme.ru/i?r=abc", policy) == "i.oneme.ru"

    async def test_plain_http_is_refused_by_default(self, policy):
        with pytest.raises(UnsafeUrl, match="plain HTTP"):
            await validate_url("http://i.oneme.ru/x", policy)

    async def test_plain_http_can_be_enabled(self, public_dns):
        loose = FetchPolicy(allow_plain_http=True)
        assert await validate_url("http://i.oneme.ru/x", loose)

    async def test_file_scheme_is_refused(self, policy):
        with pytest.raises(UnsafeUrl, match="scheme"):
            await validate_url("file:///etc/passwd", policy)

    async def test_disallowed_host_is_refused(self, policy):
        with pytest.raises(UnsafeUrl, match="allowlist"):
            await validate_url("https://evil.example/x", policy)

    async def test_private_resolution_is_refused(self, policy, monkeypatch):
        _fake_dns(monkeypatch, "10.0.0.5")
        with pytest.raises(UnsafeUrl, match="non-public"):
            await validate_url("https://i.oneme.ru/x", policy)

    async def test_loopback_literal_is_refused(self):
        loose = FetchPolicy(allow_any_host=True)
        with pytest.raises(UnsafeUrl, match="non-public"):
            await validate_url("https://127.0.0.1/x", loose)

    async def test_cloud_metadata_address_is_refused(self):
        """The classic SSRF target, refused even with the allowlist disabled."""
        loose = FetchPolicy(allow_any_host=True)
        with pytest.raises(UnsafeUrl, match="non-public"):
            await validate_url("https://169.254.169.254/latest/meta-data/", loose)

    async def test_empty_url_is_refused(self, policy):
        with pytest.raises(UnsafeUrl):
            await validate_url("", policy)

    async def test_url_without_a_host_is_refused(self, policy):
        with pytest.raises(UnsafeUrl):
            await validate_url("https:///nohost", policy)


class TestMediaSpool:
    def test_reserve_and_release_track_usage(self, spool):
        assert spool.used_bytes == 0

    async def test_budget_is_enforced(self, tmp_path):
        small = MediaSpool(str(tmp_path / "s"), max_total_bytes=100)
        await small.reserve(60)
        with pytest.raises(MediaTooLarge, match="budget"):
            await small.reserve(60)

    async def test_release_frees_the_budget(self, tmp_path):
        small = MediaSpool(str(tmp_path / "s"), max_total_bytes=100)
        fd, path = small.new_temp()
        os.write(fd, b"x" * 60)
        os.close(fd)
        await small.reserve(60)
        spooled = SpoolFile(path, "f", "application/octet-stream", 60, small)
        spooled.release()
        assert small.used_bytes == 0
        assert not os.path.exists(path)
        await small.reserve(60)      # room again

    def test_release_is_idempotent(self, spool):
        fd, path = spool.new_temp()
        os.close(fd)
        spooled = SpoolFile(path, "f", "", 0, spool)
        spooled.release()
        spooled.release()

    def test_context_manager_releases(self, spool):
        fd, path = spool.new_temp()
        os.write(fd, b"data")
        os.close(fd)
        with SpoolFile(path, "f", "", 4, spool) as spooled:
            assert spooled.read_bytes() == b"data"
        assert not os.path.exists(path)

    def test_orphans_from_a_previous_crash_are_cleaned(self, tmp_path):
        directory = tmp_path / "spool"
        directory.mkdir()
        leftover = directory / "media-stale"
        leftover.write_bytes(b"junk")
        MediaSpool(str(directory))
        assert not leftover.exists()

    def test_open_gives_a_fresh_handle_each_time(self, spool):
        fd, path = spool.new_temp()
        os.write(fd, b"abc")
        os.close(fd)
        spooled = SpoolFile(path, "f", "", 3, spool)
        try:
            with spooled.open() as first, spooled.open() as second:
                assert first.read() == second.read() == b"abc"
        finally:
            spooled.release()


class TestSniffMediaKind:
    def test_jpeg_magic_wins(self):
        assert sniff_media_kind("x.bin", "", b"\xff\xd8\xff\xe0") == "photo"

    def test_png_magic(self):
        assert sniff_media_kind("x", "", b"\x89PNG\r\n\x1a\n") == "photo"

    def test_gif_is_an_animation(self):
        assert sniff_media_kind("x", "", b"GIF89a....") == "animation"

    def test_mp4_ftyp_box(self):
        assert sniff_media_kind("x", "", b"\x00\x00\x00\x18ftypmp42") == "video"

    def test_matroska_magic(self):
        assert sniff_media_kind("x", "", b"\x1aE\xdf\xa3....") == "video"

    def test_content_type_is_the_fallback(self):
        assert sniff_media_kind("x", "image/jpeg", b"") == "photo"

    def test_extension_is_the_last_resort(self):
        assert sniff_media_kind("clip.mp4", "", b"") == "video"

    def test_avi_falls_through_to_document(self):
        """Telegram's video endpoint rejects these; document is the honest answer."""
        assert sniff_media_kind("clip.avi", "", b"") == "document"

    def test_mkv_without_magic_falls_through_to_document(self):
        assert sniff_media_kind("clip.mkv", "", b"") == "document"

    def test_bmp_falls_through_to_document(self):
        assert sniff_media_kind("pic.bmp", "", b"") == "document"

    def test_unknown_is_a_document(self):
        assert sniff_media_kind("thing", "", b"\x00\x01") == "document"


class TestLooksLikeMedia:
    def test_html_is_not_media(self):
        """A probe accepting any non-HTML 200 also accepted JSON errors."""
        assert looks_like_media("text/html; charset=utf-8") is False

    def test_json_is_not_media(self):
        assert looks_like_media("application/json") is False

    def test_empty_is_not_media(self):
        assert looks_like_media("") is False

    @pytest.mark.parametrize("ct", ["image/jpeg", "video/mp4", "audio/ogg",
                                    "application/octet-stream"])
    def test_real_media_types(self, ct):
        assert looks_like_media(ct) is True


# ── the fetcher itself, against a local server ─────────────────────

class RedirectServer:
    """Serves the handful of responses the fetch policy has to survive."""

    def __init__(self):
        self.hits: list[str] = []
        self._runner = None
        self.base = ""
        self.elsewhere = "http://blocked.example/final"

    async def start(self) -> str:
        from aiohttp import web

        async def body(request):
            self.hits.append(request.path)
            return web.Response(body=b"\xff\xd8\xff" + b"p" * 64,
                                content_type="image/jpeg")

        async def big(request):
            """Chunked, so nothing announces the size before it arrives."""
            self.hits.append(request.path)
            response = web.StreamResponse(
                headers={"Content-Type": "image/jpeg"})
            response.enable_chunked_encoding()
            await response.prepare(request)
            for _ in range(8):
                await response.write(b"x" * 4096)
            await response.write_eof()
            return response

        async def declared(request):
            """Honest Content-Length over the cap — refused before reading."""
            self.hits.append(request.path)
            return web.Response(body=b"y" * 32768, content_type="image/jpeg")

        async def to_blocked(request):
            self.hits.append(request.path)
            raise web.HTTPFound(location=self.elsewhere)

        async def loop_forever(request):
            self.hits.append(request.path)
            raise web.HTTPFound(location=f"{self.base}/loop")

        app = web.Application()
        app.router.add_get("/body", body)
        app.router.add_get("/big", big)
        app.router.add_get("/declared", declared)
        app.router.add_get("/to-blocked", to_blocked)
        app.router.add_get("/loop", loop_forever)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.base = f"http://127.0.0.1:{port}"
        return self.base

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()


@pytest.fixture
async def fetcher_env(tmp_path, monkeypatch):
    """A SafeFetcher pointed at a local server.

    ``is_public_ip`` is stubbed because the test server necessarily lives on
    loopback; the predicate itself is covered separately above. What is under
    test here is the part with no other coverage — that *every* redirect hop is
    re-checked, and that the byte cap holds against a server that misreports
    its own length (audit S05/M08, §11 item 13).
    """
    import aiohttp

    from app import net as net_module

    server = RedirectServer()
    await server.start()
    monkeypatch.setattr(net_module, "is_public_ip",
                        lambda addr: addr == "127.0.0.1")
    policy = FetchPolicy(allowed_host_suffixes=("127.0.0.1",),
                         allow_plain_http=True, max_file_bytes=8192,
                         max_redirects=2)
    spool = MediaSpool(str(tmp_path / "spool"), max_total_bytes=1024 * 1024)
    session = aiohttp.ClientSession()
    fetcher = net_module.SafeFetcher(lambda: session, policy, spool)
    try:
        yield fetcher, server, spool
    finally:
        await session.close()
        await server.stop()


class TestSafeFetcher:
    async def test_an_allowed_body_is_spooled(self, fetcher_env):
        fetcher, server, _spool = fetcher_env
        spooled = await fetcher.download(f"{server.base}/body")
        try:
            assert spooled.size == 67
            assert spooled.content_type == "image/jpeg"
        finally:
            spooled.release()

    async def test_a_redirect_to_a_blocked_host_is_refused(self, fetcher_env):
        """The first hop being allowed says nothing about the second."""
        fetcher, server, _spool = fetcher_env
        with pytest.raises(UnsafeUrl):
            await fetcher.download(f"{server.base}/to-blocked")
        assert server.hits == ["/to-blocked"]

    async def test_a_redirect_loop_is_bounded(self, fetcher_env):
        fetcher, server, _spool = fetcher_env
        with pytest.raises(UnsafeUrl):
            await fetcher.download(f"{server.base}/loop")
        assert len(server.hits) == fetcher.policy.max_redirects + 1

    async def test_a_body_over_the_cap_is_refused_mid_stream(self, fetcher_env):
        """Chunked, so the cap can only be enforced on bytes actually read."""
        fetcher, server, _spool = fetcher_env
        with pytest.raises(MediaTooLarge):
            await fetcher.download(f"{server.base}/big")

    async def test_a_declared_size_over_the_cap_is_refused_up_front(
            self, fetcher_env):
        fetcher, server, _spool = fetcher_env
        with pytest.raises(MediaTooLarge):
            await fetcher.download(f"{server.base}/declared")

    async def test_a_refused_download_leaves_no_spool_behind(self, fetcher_env):
        fetcher, server, spool = fetcher_env
        with pytest.raises(MediaTooLarge):
            await fetcher.download(f"{server.base}/big")
        assert spool.used_bytes == 0
        assert os.listdir(spool.directory) == []

    async def test_a_per_call_limit_overrides_the_policy(self, fetcher_env):
        fetcher, server, _spool = fetcher_env
        with pytest.raises(MediaTooLarge):
            await fetcher.download(f"{server.base}/body", max_bytes=8)


class TestFakeIpDns:
    """A TUN proxy on the router answers every lookup from 198.18.0.0/15."""

    FAKE_NET = (ipaddress.ip_network("198.18.0.0/15"),)

    async def test_fake_ip_is_refused_by_default_with_a_hint(self, policy,
                                                             monkeypatch):
        _fake_dns(monkeypatch, "198.18.8.96")
        with pytest.raises(UnsafeUrl, match="MEDIA_TRUSTED_NETS"):
            await validate_url("https://i.oneme.ru/x", policy)

    async def test_trusted_net_lets_an_allowlisted_host_through(self,
                                                                 monkeypatch):
        _fake_dns(monkeypatch, "198.18.8.96")
        trusting = FetchPolicy(trusted_nets=self.FAKE_NET)
        assert await validate_url("https://i.oneme.ru/x", trusting) == "i.oneme.ru"

    async def test_trusted_net_does_not_widen_the_host_allowlist(self,
                                                                 monkeypatch):
        _fake_dns(monkeypatch, "198.18.8.96")
        trusting = FetchPolicy(trusted_nets=self.FAKE_NET)
        with pytest.raises(UnsafeUrl, match="allowlist"):
            await validate_url("https://evil.example/x", trusting)

    async def test_other_private_ranges_stay_refused(self, monkeypatch):
        _fake_dns(monkeypatch, "192.168.1.1")
        trusting = FetchPolicy(trusted_nets=self.FAKE_NET)
        with pytest.raises(UnsafeUrl, match="non-public"):
            await validate_url("https://i.oneme.ru/x", trusting)
