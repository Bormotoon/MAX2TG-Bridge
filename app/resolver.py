"""Names and chat metadata for MAX IDs, with caches that expire.

Three problems the audit found in the old version, all fixed here:

* A single transient lookup failure added the user ID to ``_fetch_failed``
  **forever**, so a contact whose name failed to resolve once during a network
  blip stayed a bare number until the process restarted (audit T09). Negative
  results now expire in minutes.
* Several chats resolving the same unknown sender each issued their own RPC.
  Lookups are now single-flight: concurrent callers share one request.
* The snapshot only carries the first page of chats, and nothing ever asked MAX
  about a chat outside it — so a group could end up named after whoever happened
  to write first (audit T07). Unknown chats are fetched on demand.

Callers that need fresh data call :meth:`fetch_contact` with ``force_refresh``;
the old code called ``client.fetch_contacts()`` directly, threw the response
away and then re-read the stale cache (audit T08).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.errors import describe_exception
from app.store import chat_key

if TYPE_CHECKING:
    from app.max_client import MaxClient

log = logging.getLogger(__name__)

POSITIVE_TTL_SEC = 6 * 3600
NEGATIVE_TTL_SEC = 300
# How long a chat's metadata is trusted before ``ensure_chat`` refreshes it.
# Deliberately stale-while-revalidate: the cached title keeps being served while
# the refresh is in flight, because a chat whose name momentarily resolves to
# its own ID would be rendered as a group and could lose its topic title
# (audit T07).
CHAT_TTL_SEC = 6 * 3600
MAX_CACHE_ENTRIES = 5000
# MAX has never documented a batch ceiling; this keeps one request from growing
# without bound while staying well inside anything plausible.
CONTACT_BATCH_SIZE = 100


@dataclass
class _Entry:
    value: Any
    expires_at: float

    @property
    def fresh(self) -> bool:
        return time.monotonic() < self.expires_at


class _TtlCache:
    def __init__(self, ttl: float, max_entries: int = MAX_CACHE_ENTRIES):
        self._ttl = ttl
        self._max = max_entries
        self._data: dict[Any, _Entry] = {}

    def get(self, key: Any) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if not entry.fresh:
            self._data.pop(key, None)
            return None
        return entry.value

    def has(self, key: Any) -> bool:
        return self.get(key) is not None

    def put(self, key: Any, value: Any) -> None:
        if len(self._data) >= self._max:
            # Drop the soonest-to-expire entry: cheap, and bounded is the point.
            oldest = min(self._data, key=lambda k: self._data[k].expires_at)
            self._data.pop(oldest, None)
        self._data[key] = _Entry(value, time.monotonic() + self._ttl)

    def drop(self, key: Any) -> None:
        self._data.pop(key, None)

    def clear(self) -> None:
        self._data.clear()

    def items(self) -> dict:
        return {k: e.value for k, e in self._data.items() if e.fresh}

    def __len__(self) -> int:
        return len(self._data)


def is_contact_shape(obj: Any) -> bool:
    """Whether a dict plausibly describes a MAX user.

    The old deep-walk accepted any object with an ``id`` and something
    name-shaped, which meant an unrelated nested structure could be filed away
    as a contact (audit T09).
    """
    if not isinstance(obj, dict):
        return False
    if obj.get("id") is None and obj.get("userId") is None:
        return False
    if isinstance(obj.get("names"), list):
        return True
    return any(k in obj for k in ("firstName", "first_name", "lastName",
                                  "last_name", "displayName"))


def extract_name(contact: dict) -> str:
    """Pull a display name out of the several shapes MAX uses."""
    names_list = contact.get("names")
    if isinstance(names_list, list) and names_list:
        first_entry = names_list[0]
        if isinstance(first_entry, dict):
            first = first_entry.get("firstName") or ""
            last = first_entry.get("lastName") or ""
            if first or last:
                return f"{first} {last}".strip()
            if first_entry.get("name"):
                return str(first_entry["name"])

    first = contact.get("firstName") or contact.get("first_name") or ""
    last = contact.get("lastName") or contact.get("last_name") or ""
    if first or last:
        return f"{first} {last}".strip()

    return str(contact.get("friendly") or contact.get("displayName")
               or contact.get("name") or "")


class ContactResolver:
    def __init__(self, client: MaxClient | None = None):
        self._client = client
        self._my_id: Any = None
        self._names = _TtlCache(POSITIVE_TTL_SEC)
        self._contacts = _TtlCache(POSITIVE_TTL_SEC)
        self._failed = _TtlCache(NEGATIVE_TTL_SEC)
        self._chat_titles: dict[str, str] = {}
        self._chat_types: dict[str, str] = {}
        self._chats_raw: dict[str, dict] = {}
        self._chat_fetched_at: dict[str, float] = {}
        self._inflight: dict[Any, asyncio.Future] = {}
        self._chat_inflight: dict[str, asyncio.Future] = {}

    # ── read-only views ────────────────────────────────────────────

    @property
    def my_id(self) -> Any:
        return self._my_id

    @property
    def users(self) -> dict:
        return self._names.items()

    @property
    def chats(self) -> dict:
        return dict(self._chat_titles)

    @property
    def contacts_raw(self) -> dict:
        return self._contacts.items()

    def chat_raw(self, chat_id: Any) -> dict:
        return self._chats_raw.get(chat_key(chat_id), {})

    def chat_name(self, chat_id: Any) -> str:
        return self._chat_titles.get(chat_key(chat_id), str(chat_id))

    def chat_type(self, chat_id: Any) -> str | None:
        return self._chat_types.get(chat_key(chat_id))

    def is_dm(self, chat_id: Any) -> bool:
        return self._chat_types.get(chat_key(chat_id)) == "DIALOG"

    def user_name(self, user_id: Any) -> str:
        return self._names.get(user_id) or str(user_id)

    def contact(self, user_id: Any) -> dict | None:
        return self._contacts.get(user_id)

    def peer_in_dm(self, chat_id: Any) -> Any | None:
        """The other participant of a DIALOG, normalised to an int when possible."""
        chat = self.chat_raw(chat_id)
        for uid in (chat.get("participants") or {}):
            try:
                value: Any = int(uid)
            except (TypeError, ValueError):
                value = uid
            if chat_key(value) != chat_key(self._my_id):
                return value
        return None

    # ── name resolution ────────────────────────────────────────────

    async def resolve_user(self, user_id: Any) -> str:
        """Human name for a user ID, falling back to the ID itself.

        Channel posts arrive with no sender at all; passing that into
        CONTACT_GET makes MAX reject the frame and close the socket, so it is
        short-circuited here.
        """
        if user_id is None:
            return ""
        cached = self._names.get(user_id)
        if cached:
            return cached
        if self._failed.has(user_id):
            return str(user_id)

        await self._fetch_contacts_single_flight([user_id])
        return self._names.get(user_id) or str(user_id)

    async def resolve_users_batch(self, user_ids: list) -> None:
        unknown = [uid for uid in user_ids
                   if uid is not None and not self._names.has(uid)
                   and not self._failed.has(uid)]
        for start in range(0, len(unknown), CONTACT_BATCH_SIZE):
            await self._fetch_contacts_single_flight(
                unknown[start:start + CONTACT_BATCH_SIZE])

    async def fetch_contact(self, user_id: Any, *,
                            force_refresh: bool = False) -> dict | None:
        """Fetch one contact, optionally bypassing the cache.

        ``/intro`` promises a refreshed card; without this it re-read whatever
        was already cached and the avatar never changed (audit T08).
        """
        if user_id is None:
            return None
        if force_refresh:
            self._contacts.drop(user_id)
            self._names.drop(user_id)
            self._failed.drop(user_id)
        elif (cached := self._contacts.get(user_id)) is not None:
            return cached
        await self._fetch_contacts_single_flight([user_id])
        return self._contacts.get(user_id)

    async def _fetch_contacts_single_flight(self, user_ids: list) -> None:
        """One in-flight request per user; concurrent callers share it."""
        if not self._client or not user_ids:
            return
        pending = [uid for uid in user_ids if uid not in self._inflight]
        waiters = [self._inflight[uid] for uid in user_ids if uid in self._inflight]

        if pending:
            loop = asyncio.get_running_loop()
            future: asyncio.Future = loop.create_future()
            for uid in pending:
                self._inflight[uid] = future
            try:
                resp = await self._client.fetch_contacts(list(pending))
                self.ingest_contacts(resp)
                for uid in pending:
                    if not self._names.has(uid):
                        self._failed.put(uid, True)
                if not future.done():
                    future.set_result(None)
            except Exception as exc:
                log.info("Contact lookup failed for %d id(s): %s",
                         len(pending), describe_exception(exc))
                for uid in pending:
                    # Short negative TTL, so a blip does not blank a name until
                    # the next restart.
                    self._failed.put(uid, True)
                if not future.done():
                    future.set_result(None)
            finally:
                for uid in pending:
                    if self._inflight.get(uid) is future:
                        self._inflight.pop(uid, None)

        for waiter in waiters:
            try:
                await waiter
            except Exception:
                pass

    def ingest_contacts(self, resp: Any) -> int:
        """Parse a CONTACT_GET response in any of the shapes MAX returns."""
        if not isinstance(resp, dict):
            return 0
        found = 0

        contacts = resp.get("contacts") or resp.get("users") or []
        if isinstance(contacts, dict):
            contacts = list(contacts.values())
        if not isinstance(contacts, list):
            contacts = []

        candidates = list(contacts)
        if is_contact_shape(resp):
            candidates.append(resp)          # the response *is* the contact

        for candidate in candidates:
            found += self._store_contact(candidate)

        if not found:
            found += self._deep_extract(resp, depth=0)
        return found

    def _store_contact(self, contact: Any) -> int:
        if not is_contact_shape(contact):
            return 0
        uid = contact.get("id") if contact.get("id") is not None else contact.get("userId")
        name = extract_name(contact)
        # Always refresh the raw record: the old code skipped it when a name was
        # already known, so `contacts_raw` stayed empty for users whose name came
        # from the snapshot (audit T08).
        self._contacts.put(uid, contact)
        self._failed.drop(uid)
        if name:
            self._names.put(uid, name)
            return 1
        return 0

    def _deep_extract(self, obj: Any, depth: int) -> int:
        if depth > 5:
            return 0
        found = 0
        if isinstance(obj, dict):
            if is_contact_shape(obj):
                found += self._store_contact(obj)
            for value in obj.values():
                found += self._deep_extract(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                found += self._deep_extract(item, depth + 1)
        return found

    # ── chat metadata ──────────────────────────────────────────────

    def load_snapshot(self, snapshot: dict) -> list:
        """Seed the caches from AUTH_SNAPSHOT and list participants to resolve."""
        profile = snapshot.get("profile") or {}
        self._my_id = profile.get("id")
        my_name = extract_name(profile)
        if my_name and self._my_id is not None:
            self._names.put(self._my_id, my_name)

        participant_ids: set[Any] = set()
        chats = snapshot.get("chats")
        if not isinstance(chats, list):
            chats = []

        for chat in chats:
            if not isinstance(chat, dict):
                continue
            participant_ids.update(self.ingest_chat(chat))

        log.info("Snapshot parsed: %d chat(s), %d participant id(s) to resolve",
                 len(self._chat_titles), len(participant_ids))
        return list(participant_ids)

    def ingest_chat(self, chat: dict) -> set:
        """Record one chat object; returns the participant IDs it mentions."""
        cid = chat.get("id")
        if cid is None:
            return set()
        key = chat_key(cid)
        self._evict_chats_if_full(key)
        self._chats_raw[key] = chat
        self._chat_fetched_at[key] = time.monotonic()

        ctype = chat.get("type")
        if ctype:
            self._chat_types[key] = str(ctype)
        title = chat.get("title")
        if title:
            self._chat_titles[key] = str(title)

        participants = chat.get("participants")
        if not isinstance(participants, dict):
            participants = {}
        ids: set[Any] = set()
        for uid in participants:
            try:
                ids.add(int(uid))
            except (TypeError, ValueError):
                ids.add(uid)

        if not title and ctype == "DIALOG" and self._my_id is not None:
            peer = next((uid for uid in ids
                         if chat_key(uid) != chat_key(self._my_id)), None)
            if peer is not None:
                # A placeholder, and marked as one, so a real name replaces it
                # later instead of being treated as final (audit T07).
                self._chat_titles[key] = f"DM:{peer}"
        return ids

    async def ensure_chat(self, chat_id: Any) -> dict:
        """Fetch a chat MAX did not include in the snapshot page.

        Single-flight for the same reason contact lookups are: a burst of
        messages in a new chat should ask once.
        """
        key = chat_key(chat_id)
        known = self._chats_raw.get(key)
        if known and not self._chat_is_stale(key):
            return known
        if not self._client:
            return known or {}

        inflight = self._chat_inflight.get(key)
        if inflight is not None:
            if known:
                # A refresh is already running; serving the value we have beats
                # making this message wait for it.
                return known
            try:
                await inflight
            except Exception:
                pass
            return self._chats_raw.get(key, {})

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._chat_inflight[key] = future
        try:
            resp = await self._client.fetch_chat(chat_id)
            chats = (resp or {}).get("chats") or []
            if isinstance(chats, dict):
                chats = list(chats.values())
            for chat in chats:
                if isinstance(chat, dict):
                    self.ingest_chat(chat)
            if isinstance(resp, dict) and isinstance(resp.get("chat"), dict):
                self.ingest_chat(resp["chat"])
        except Exception as exc:
            log.info("CHAT_GET for %s failed: %s", key, describe_exception(exc))
            if known:
                # Back off for a full TTL rather than re-asking on every
                # message while MAX is unreachable; the stale copy still works.
                self._chat_fetched_at[key] = time.monotonic()
        finally:
            if not future.done():
                future.set_result(None)
            self._chat_inflight.pop(key, None)
        return self._chats_raw.get(key, known or {})

    def _chat_is_stale(self, key: str) -> bool:
        fetched = self._chat_fetched_at.get(key)
        return fetched is None or (time.monotonic() - fetched) > CHAT_TTL_SEC

    def _evict_chats_if_full(self, incoming: str) -> None:
        """Keep the chat caches bounded, dropping the least recently fetched."""
        if incoming in self._chats_raw or len(self._chats_raw) < MAX_CACHE_ENTRIES:
            return
        oldest = min(self._chats_raw,
                     key=lambda k: self._chat_fetched_at.get(k, 0.0))
        for cache in (self._chats_raw, self._chat_titles, self._chat_types,
                      self._chat_fetched_at):
            cache.pop(oldest, None)

    def forget_transient_failures(self) -> None:
        """Clear negative caches after a reconnect — the cause may be gone."""
        self._failed.clear()
