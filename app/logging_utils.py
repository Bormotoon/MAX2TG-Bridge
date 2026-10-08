"""Log redaction, applied before anything reaches a handler.

At ``LOG_LEVEL=DEBUG`` the old code wrote the outgoing AUTH frame — token and
all — into the rotating file, and at INFO it logged message text, contact names
and signed media URLs whose query string is itself a capability (audit S04). A
log file is not a place to keep any of that.

Redaction lives in a ``logging.Filter`` rather than at the call sites because
call sites are where it gets forgotten. Secrets are registered once at startup;
anything that later flows through a log record — message, args, or an
exception's text — is rewritten.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlsplit

REDACTED = "«redacted»"

# Values registered at startup (tokens, device IDs). Module-level so a late
# logger created anywhere still gets a filter that knows about them.
_SECRETS: set[str] = set()

# Fields whose *value* is a secret wherever they appear in a serialised frame.
_SECRET_KEY_RE = re.compile(
    r'("(?:token|accessToken|refreshToken|auth|authToken|password|secret|'
    r'deviceId|photoToken|fileToken)"\s*:\s*)"[^"]*"',
    re.IGNORECASE,
)

# Query strings on CDN URLs carry the signature that makes the URL usable.
_URL_QUERY_RE = re.compile(r'(https?://[^\s"\'<>]+?)\?[^\s"\'<>]*')


def register_secret(value: str | None, *, min_length: int = 8) -> None:
    """Teach the redactor about one secret value."""
    if value and len(value) >= min_length:
        _SECRETS.add(value)


def register_secrets(values: Iterable[str | None]) -> None:
    for value in values:
        register_secret(value)


def clear_secrets() -> None:
    """Test helper — production never un-registers a secret."""
    _SECRETS.clear()


def redact(text: str) -> str:
    """Strip registered secrets, secret-looking JSON fields and URL queries."""
    if not text:
        return text
    for secret in _SECRETS:
        if secret in text:
            text = text.replace(secret, REDACTED)
    text = _SECRET_KEY_RE.sub(lambda m: f'{m.group(1)}"{REDACTED}"', text)
    text = _URL_QUERY_RE.sub(lambda m: f"{m.group(1)}?{REDACTED}", text)
    return text


def safe_url(url: Any) -> str:
    """Host and path only — the part that helps debugging, minus the credential."""
    if not isinstance(url, str) or not url:
        return "<no url>"
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<malformed url>"
    if not parts.hostname:
        return "<malformed url>"
    path = parts.path or "/"
    if len(path) > 60:
        path = path[:57] + "..."
    return f"{parts.scheme}://{parts.hostname}{path}"


# Printf-style conversion specifiers, so we can tell whether redacting a
# template destroyed one.
_PLACEHOLDER_RE = re.compile(
    r"%(?:\(\w*\))?[-+ #0]*[\d*]*(?:\.[\d*]+)?[hlL]?[diouxXeEfFgGcrsa]")


class RedactingFilter(logging.Filter):
    """Rewrites every record's message and args on the way to the handlers."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                cleaned = redact(record.msg)
                if record.args and (_PLACEHOLDER_RE.findall(cleaned)
                                    != _PLACEHOLDER_RE.findall(record.msg)):
                    # Redaction swallowed a placeholder the record still has an
                    # argument for — `"token": "%s"` collapses to
                    # `"token": "«redacted»"`. Formatting would then raise and
                    # the line would never reach the file, which is a strange
                    # way to protect a secret. Interpolate first, redact the
                    # finished line, and leave nothing for % to do.
                    record.msg = redact(record.getMessage())
                    record.args = ()
                    return True
                record.msg = cleaned
            if record.args:
                if isinstance(record.args, dict):
                    record.args = {k: self._clean(v) for k, v in record.args.items()}
                else:
                    record.args = tuple(self._clean(a) for a in record.args)
            if record.exc_text:
                record.exc_text = redact(record.exc_text)
        except Exception:  # a broken filter must never drop a log line
            return True
        return True

    @staticmethod
    def _clean(value: Any) -> Any:
        return redact(value) if isinstance(value, str) else value


def install(handlers: Iterable[logging.Handler]) -> None:
    """Attach the filter to every handler on the root logger."""
    redactor = RedactingFilter()
    for handler in handlers:
        handler.addFilter(redactor)


class TextPolicy:
    """Decides whether message bodies may appear in the log at all.

    Default is no: an ID, a type, a size and a latency are enough to debug a
    delivery problem, and they are not somebody's private conversation. Turning
    it on is a deliberate act with a warning attached.
    """

    def __init__(self, log_message_text: bool = False):
        self.log_message_text = log_message_text

    def preview(self, text: str | None, limit: int = 80) -> str:
        if not text:
            return "<empty>"
        if not self.log_message_text:
            return f"<{len(text)} chars>"
        clipped = text[:limit]
        return redact(clipped + ("…" if len(text) > limit else ""))

    def name(self, value: Any) -> str:
        if self.log_message_text:
            return str(value)
        return f"<name:{len(str(value))} chars>"


# The bridge replaces this at startup; the conservative default applies to any
# code that logs before configuration has run.
policy = TextPolicy(log_message_text=False)


def set_policy(new_policy: TextPolicy) -> None:
    global policy
    policy = new_policy
