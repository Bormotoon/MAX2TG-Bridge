"""Contact and chat caches: TTLs, single-flight, and strict contact shapes."""

from __future__ import annotations

import asyncio
import time

from app.resolver import (
    NEGATIVE_TTL_SEC,
    ContactResolver,
    _TtlCache,
    extract_name,
    is_contact_shape,
)


class FakeClient:
    """Counts lookups so single-flight behaviour is observable."""

    def __init__(self, response=None, error: Exception | None = None):
        self.response = response if response is not None else {}
        self.error = error
        self.calls: list[list] = []
        self.gate: asyncio.Event | None = None
        self.chat_response: dict = {}
        self.chat_calls: list = []

    async def fetch_contacts(self, ids):
        self.calls.append(list(ids))
        if self.gate is not None:
            await self.gate.wait()
        if self.error:
            raise self.error
        return self.response

    async def fetch_chat(self, chat_id):
        self.chat_calls.append(chat_id)
        return self.chat_response


def contact(uid: int, first="Алиса", last="А"):
    return {"id": uid, "names": [{"firstName": first, "lastName": last}]}


class TestTtlCache:
    def test_value_is_returned_while_fresh(self):
        cache = _TtlCache(ttl=10)
        cache.put("k", "v")
        assert cache.get("k") == "v"

    def test_expired_value_is_gone(self):
        cache = _TtlCache(ttl=-1)
        cache.put("k", "v")
        assert cache.get("k") is None

    def test_cache_is_bounded(self):
        cache = _TtlCache(ttl=100, max_entries=3)
        for i in range(10):
            cache.put(i, i)
        assert len(cache) <= 3

    def test_drop_removes_an_entry(self):
        cache = _TtlCache(ttl=10)
        cache.put("k", "v")
        cache.drop("k")
        assert cache.get("k") is None


class TestContactShape:
    def test_a_real_contact_is_recognised(self):
        assert is_contact_shape(contact(1)) is True

    def test_flat_name_fields_count(self):
        assert is_contact_shape({"id": 1, "firstName": "A"}) is True

    def test_an_object_without_an_id_is_not_a_contact(self):
        assert is_contact_shape({"names": [{"firstName": "A"}]}) is False

    def test_an_arbitrary_object_with_an_id_is_not_a_contact(self):
        """The old deep-walk filed unrelated structures away as contacts."""
        assert is_contact_shape({"id": 5, "size": 100, "url": "x"}) is False

    def test_non_dict_is_not_a_contact(self):
        assert is_contact_shape("nope") is False


class TestExtractName:
    def test_names_array(self):
        assert extract_name(contact(1, "Алиса", "Петрова")) == "Алиса Петрова"

    def test_names_array_with_only_a_name_field(self):
        assert extract_name({"names": [{"name": "Канал"}]}) == "Канал"

    def test_flat_fields(self):
        assert extract_name({"firstName": "Боб", "lastName": "Ли"}) == "Боб Ли"

    def test_snake_case_fields(self):
        assert extract_name({"first_name": "Боб"}) == "Боб"

    def test_display_name_fallback(self):
        assert extract_name({"displayName": "Некто"}) == "Некто"

    def test_nothing_usable(self):
        assert extract_name({"id": 1}) == ""


class TestResolveUser:
    async def test_a_known_contact_is_returned(self):
        client = FakeClient({"contacts": [contact(7)]})
        resolver = ContactResolver(client=client)
        assert await resolver.resolve_user(7) == "Алиса А"

    async def test_none_short_circuits(self):
        """A channel post has no sender; asking MAX for it closes the socket."""
        client = FakeClient()
        resolver = ContactResolver(client=client)
        assert await resolver.resolve_user(None) == ""
        assert client.calls == []

    async def test_unknown_user_falls_back_to_the_id(self):
        resolver = ContactResolver(client=FakeClient({}))
        assert await resolver.resolve_user(7) == "7"

    async def test_the_cache_prevents_a_second_lookup(self):
        client = FakeClient({"contacts": [contact(7)]})
        resolver = ContactResolver(client=client)
        await resolver.resolve_user(7)
        await resolver.resolve_user(7)
        assert len(client.calls) == 1

    async def test_a_failure_is_cached_only_briefly(self):
        """A single blip used to blank a name until the process restarted."""
        client = FakeClient(error=RuntimeError("network"))
        resolver = ContactResolver(client=client)
        await resolver.resolve_user(7)
        assert resolver._failed.get(7) is True
        assert NEGATIVE_TTL_SEC <= 600

    async def test_transient_failures_can_be_forgotten_on_reconnect(self):
        client = FakeClient(error=RuntimeError("network"))
        resolver = ContactResolver(client=client)
        await resolver.resolve_user(7)
        resolver.forget_transient_failures()
        client.error = None
        client.response = {"contacts": [contact(7)]}
        assert await resolver.resolve_user(7) == "Алиса А"

    async def test_concurrent_lookups_share_one_request(self):
        client = FakeClient({"contacts": [contact(7)]})
        client.gate = asyncio.Event()
        resolver = ContactResolver(client=client)
        tasks = [asyncio.create_task(resolver.resolve_user(7)) for _ in range(5)]
        await asyncio.sleep(0.01)
        client.gate.set()
        results = await asyncio.gather(*tasks)
        assert results == ["Алиса А"] * 5
        assert len(client.calls) == 1

    async def test_batch_resolution_skips_known_users(self):
        client = FakeClient({"contacts": [contact(1), contact(2)]})
        resolver = ContactResolver(client=client)
        await resolver.resolve_users_batch([1, 2])
        await resolver.resolve_users_batch([1, 2])
        assert len(client.calls) == 1

    async def test_batches_are_chunked(self):
        client = FakeClient({})
        resolver = ContactResolver(client=client)
        await resolver.resolve_users_batch(list(range(250)))
        assert len(client.calls) == 3
        assert all(len(c) <= 100 for c in client.calls)


class TestFetchContact:
    async def test_force_refresh_hits_the_server_again(self):
        """``/intro`` promised a refresh and re-read the cache instead (T08)."""
        client = FakeClient({"contacts": [contact(7)]})
        resolver = ContactResolver(client=client)
        await resolver.fetch_contact(7)
        await resolver.fetch_contact(7, force_refresh=True)
        assert len(client.calls) == 2

    async def test_raw_contact_is_stored_even_when_the_name_was_known(self):
        client = FakeClient({"contacts": [contact(7)]})
        resolver = ContactResolver(client=client)
        resolver._names.put(7, "Алиса А")
        await resolver.fetch_contact(7)
        assert resolver.contact(7) is not None

    async def test_none_id(self):
        resolver = ContactResolver(client=FakeClient())
        assert await resolver.fetch_contact(None) is None


class TestIngestContacts:
    def test_list_shape(self):
        resolver = ContactResolver()
        assert resolver.ingest_contacts({"contacts": [contact(1)]}) == 1

    def test_dict_shape(self):
        resolver = ContactResolver()
        assert resolver.ingest_contacts({"users": {"a": contact(1)}}) == 1

    def test_response_is_itself_the_contact(self):
        resolver = ContactResolver()
        assert resolver.ingest_contacts(contact(1)) == 1
        assert resolver.user_name(1) == "Алиса А"

    def test_nested_contact_is_found(self):
        resolver = ContactResolver()
        assert resolver.ingest_contacts({"payload": {"deep": [contact(3)]}}) == 1

    def test_non_dict_response(self):
        assert ContactResolver().ingest_contacts(None) == 0


class TestSnapshot:
    def test_profile_and_chats_are_recorded(self):
        resolver = ContactResolver()
        ids = resolver.load_snapshot({
            "profile": {"id": 100, "names": [{"firstName": "Я"}]},
            "chats": [{"id": -1, "type": "CHAT", "title": "Команда",
                       "participants": {"100": 1, "200": 1}}],
        })
        assert resolver.my_id == 100
        assert resolver.user_name(100) == "Я"
        assert resolver.chat_name(-1) == "Команда"
        assert set(ids) == {100, 200}

    def test_dialog_without_a_title_gets_a_placeholder(self):
        resolver = ContactResolver()
        resolver.load_snapshot({
            "profile": {"id": 100},
            "chats": [{"id": -2, "type": "DIALOG",
                       "participants": {"100": 1, "300": 1}}],
        })
        assert resolver.chat_name(-2) == "DM:300"
        assert resolver.is_dm(-2) is True

    def test_malformed_chats_are_skipped(self):
        resolver = ContactResolver()
        resolver.load_snapshot({"profile": {"id": 1},
                                "chats": ["garbage", None, {"no": "id"}]})
        assert resolver.chats == {}

    def test_chats_not_a_list(self):
        resolver = ContactResolver()
        assert resolver.load_snapshot({"profile": {"id": 1}, "chats": "x"}) == []

    def test_peer_in_dm(self):
        resolver = ContactResolver()
        resolver.load_snapshot({
            "profile": {"id": 100},
            "chats": [{"id": -2, "type": "DIALOG",
                       "participants": {"100": 1, "300": 1}}],
        })
        assert resolver.peer_in_dm(-2) == 300

    def test_chat_id_type_does_not_matter(self):
        resolver = ContactResolver()
        resolver.load_snapshot({"profile": {"id": 1},
                                "chats": [{"id": -5, "title": "T", "type": "CHAT"}]})
        assert resolver.chat_name("-5") == "T"


class TestEnsureChat:
    async def test_known_chat_needs_no_request(self):
        client = FakeClient()
        resolver = ContactResolver(client=client)
        resolver.ingest_chat({"id": -1, "title": "T", "type": "CHAT"})
        assert (await resolver.ensure_chat(-1))["title"] == "T"

    async def test_unknown_chat_is_fetched(self):
        """A chat outside the snapshot page had no title at all (audit T07)."""
        client = FakeClient()
        client.chat_response = {"chats": [{"id": -9, "title": "Новый",
                                           "type": "CHAT"}]}
        resolver = ContactResolver(client=client)
        await resolver.ensure_chat(-9)
        assert resolver.chat_name(-9) == "Новый"

    async def test_failure_leaves_the_chat_unknown_without_raising(self):
        class Boom(FakeClient):
            async def fetch_chat(self, chat_id):
                raise RuntimeError("no")

        resolver = ContactResolver(client=Boom())
        assert await resolver.ensure_chat(-9) == {}

    async def test_no_client(self):
        assert await ContactResolver().ensure_chat(-1) == {}


class TestChatMetadataTtl:
    """Chat metadata expires so a rename is picked up — without ever going blank.

    Contacts already aged out; chat titles and types were plain dicts that lived
    for the life of the process, so a chat renamed in MAX kept its old topic name
    until a restart (audit T07). Expiry is stale-while-revalidate on purpose: a
    title that briefly resolved to the chat's own ID would make ``is_dm`` false
    and re-render a private chat as a group.
    """

    @staticmethod
    def _stale(resolver, chat_id=-1):
        from app.resolver import CHAT_TTL_SEC
        resolver._chat_fetched_at[str(chat_id)] = (
            time.monotonic() - CHAT_TTL_SEC - 1)

    async def test_a_fresh_chat_is_not_refetched(self):
        client = FakeClient()
        resolver = ContactResolver(client=client)
        resolver.ingest_chat({"id": -1, "title": "T", "type": "CHAT"})
        await resolver.ensure_chat(-1)
        assert client.chat_calls == []

    async def test_a_stale_chat_is_refetched_and_the_rename_lands(self):
        client = FakeClient()
        client.chat_response = {"chats": [{"id": -1, "title": "Новое имя",
                                           "type": "CHAT"}]}
        resolver = ContactResolver(client=client)
        resolver.ingest_chat({"id": -1, "title": "Старое", "type": "CHAT"})
        self._stale(resolver)
        await resolver.ensure_chat(-1)
        assert resolver.chat_name(-1) == "Новое имя"

    async def test_a_failed_refresh_keeps_the_old_metadata(self):
        """Serving a stale title beats rendering a DM as a numbered group."""
        class Boom(FakeClient):
            async def fetch_chat(self, chat_id):
                raise RuntimeError("MAX unreachable")

        resolver = ContactResolver(client=Boom())
        resolver.ingest_chat({"id": -1, "title": "Алиса", "type": "DIALOG"})
        self._stale(resolver)
        assert (await resolver.ensure_chat(-1))["title"] == "Алиса"
        assert resolver.chat_name(-1) == "Алиса"
        assert resolver.is_dm(-1) is True

    async def test_a_failed_refresh_backs_off_instead_of_retrying_every_message(self):
        class Boom(FakeClient):
            def __init__(self):
                super().__init__()
                self.tries = 0

            async def fetch_chat(self, chat_id):
                self.tries += 1
                raise RuntimeError("MAX unreachable")

        client = Boom()
        resolver = ContactResolver(client=client)
        resolver.ingest_chat({"id": -1, "title": "Алиса", "type": "DIALOG"})
        self._stale(resolver)
        for _ in range(5):
            await resolver.ensure_chat(-1)
        assert client.tries == 1

    async def test_the_chat_caches_are_bounded(self):
        from app import resolver as resolver_module

        resolver = ContactResolver()
        original, resolver_module.MAX_CACHE_ENTRIES = (
            resolver_module.MAX_CACHE_ENTRIES, 3)
        try:
            for i in range(10):
                resolver.ingest_chat({"id": -i, "title": f"T{i}", "type": "CHAT"})
        finally:
            resolver_module.MAX_CACHE_ENTRIES = original
        assert len(resolver._chats_raw) <= 3
        assert len(resolver._chat_titles) <= 3
        assert len(resolver._chat_fetched_at) <= 3
