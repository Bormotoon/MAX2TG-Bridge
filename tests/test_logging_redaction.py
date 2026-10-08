"""Nothing secret reaches a log handler — the sentinel-token check from audit §11."""

from __future__ import annotations

import logging

import pytest

from app import logging_utils
from app.logging_utils import (
    REDACTED,
    RedactingFilter,
    TextPolicy,
    redact,
    register_secret,
    safe_url,
)

SENTINEL = "sentinel-token-value-9f3a1c"


@pytest.fixture(autouse=True)
def clean_secrets():
    logging_utils.clear_secrets()
    yield
    logging_utils.clear_secrets()


class TestRedact:
    def test_registered_secret_is_replaced(self):
        register_secret(SENTINEL)
        assert SENTINEL not in redact(f"auth with {SENTINEL} ok")
        assert REDACTED in redact(f"auth with {SENTINEL} ok")

    def test_unregistered_short_values_are_not_registered(self):
        register_secret("abc")
        assert redact("abc") == "abc"

    def test_token_json_field_is_redacted_even_if_unknown(self):
        """The AUTH frame is redacted structurally, not only by known value."""
        raw = '{"opcode":19,"payload":{"token":"whatever-it-is","chatsCount":10}}'
        cleaned = redact(raw)
        assert "whatever-it-is" not in cleaned
        assert "chatsCount" in cleaned

    @pytest.mark.parametrize("field", [
        "token", "accessToken", "refreshToken", "password", "secret",
        "deviceId", "photoToken", "fileToken",
    ])
    def test_all_secret_fields_are_covered(self, field):
        assert "hunter2" not in redact(f'{{"{field}": "hunter2"}}')

    def test_signed_url_query_is_stripped(self):
        """The query string on a CDN URL is the capability, not a detail."""
        cleaned = redact("downloading https://i.oneme.ru/i?r=SIGNATURE&t=9 now")
        assert "SIGNATURE" not in cleaned
        assert "i.oneme.ru" in cleaned

    def test_empty_input(self):
        assert redact("") == ""


class TestFilterIntegration:
    def _capture(self, logger, emit):
        records: list[str] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(self.format(record))

        handler = Capture()
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.addFilter(RedactingFilter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            emit()
        finally:
            logger.removeHandler(handler)
        return records

    def test_message_body_is_redacted(self):
        register_secret(SENTINEL)
        logger = logging.getLogger("test.redaction.msg")
        records = self._capture(logger, lambda: logger.info(
            "sending auth %s", SENTINEL))
        assert SENTINEL not in records[0]

    def test_args_are_redacted(self):
        register_secret(SENTINEL)
        logger = logging.getLogger("test.redaction.args")
        records = self._capture(logger, lambda: logger.info(
            "token=%s url=%s", SENTINEL, "https://i.oneme.ru/i?r=SIG"))
        assert SENTINEL not in records[0]
        assert "SIG" not in records[0]

    def test_dict_args_are_redacted(self):
        register_secret(SENTINEL)
        logger = logging.getLogger("test.redaction.dict")
        records = self._capture(logger, lambda: logger.info(
            "%(t)s", {"t": SENTINEL}))
        assert SENTINEL not in records[0]

    def test_non_string_args_pass_through(self):
        logger = logging.getLogger("test.redaction.nums")
        records = self._capture(logger, lambda: logger.info("n=%d", 42))
        assert "n=42" in records[0]

    def test_a_filter_failure_never_drops_a_record(self):
        record = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)
        record.args = object()       # not a tuple or dict
        assert RedactingFilter().filter(record) is True


class TestSafeUrl:
    def test_query_is_dropped(self):
        assert safe_url("https://i.oneme.ru/i?r=SECRET") == "https://i.oneme.ru/i"

    def test_long_paths_are_clipped(self):
        result = safe_url("https://i.oneme.ru/" + "a" * 200)
        assert len(result) < 100 and result.endswith("...")

    def test_non_string_input(self):
        assert safe_url(None) == "<no url>"

    def test_malformed_url(self):
        assert safe_url("::::") == "<malformed url>"


class TestTextPolicy:
    def test_bodies_are_hidden_by_default(self):
        policy = TextPolicy(log_message_text=False)
        assert policy.preview("секретное сообщение") == "<19 chars>"

    def test_bodies_can_be_enabled_deliberately(self):
        policy = TextPolicy(log_message_text=True)
        assert "привет" in policy.preview("привет")

    def test_preview_clips_and_marks(self):
        policy = TextPolicy(log_message_text=True)
        assert policy.preview("x" * 200, limit=10).endswith("…")

    def test_empty_text(self):
        assert TextPolicy(True).preview("") == "<empty>"

    def test_names_are_hidden_by_default(self):
        assert TextPolicy(False).name("Алиса") == "<name:5 chars>"

    def test_module_default_is_conservative(self):
        assert logging_utils.policy.log_message_text is False


class TestTheRealLogFile:
    """The sentinel check the audit actually asked for (§11, item 12).

    Everything above tests ``redact`` and the filter on their own. This drives
    the real ``setup_logging`` wiring and then reads the file off disk, because
    the failure mode that matters is not a broken regex — it is a handler that
    never got the filter attached, at the one log level where the token used to
    appear (audit S04).
    """

    @pytest.fixture
    def configured(self, tmp_path):
        from app.config import Settings
        from app.main import setup_logging

        root = logging.getLogger()
        saved = (list(root.handlers), root.level)
        settings = Settings(
            max_token=SENTINEL,
            max_device_id="device-sentinel-8b21e4",
            tg_bot_token="1234567:bot-sentinel-77c0aa",
            tg_chat_id=-1001234567890,
            state_dir=str(tmp_path), spool_dir=str(tmp_path / "spool"),
            log_dir=str(tmp_path), debug=True, log_level="DEBUG",
        )
        setup_logging(settings)
        try:
            yield settings, tmp_path / "max2tg.log"
        finally:
            for handler in list(root.handlers):
                handler.close()
            root.handlers, root.level = saved

    @staticmethod
    def _contents(path) -> str:
        for handler in logging.getLogger().handlers:
            handler.flush()
        return path.read_text(encoding="utf-8")

    def test_the_auth_frame_never_reaches_the_file_at_debug(self, configured):
        settings, path = configured
        log = logging.getLogger("test.auth")
        frame = '{"opcode": 19, "payload": {"token": "' + settings.max_token + '"}}'
        log.debug(">>> SEND op=19 raw=%s", frame)
        body = self._contents(path)
        assert settings.max_token not in body
        assert REDACTED in body

    def test_a_template_whose_placeholder_is_redacted_still_logs(self, configured):
        """Redacting `"token": "%s"` removes the placeholder its arg needs.

        The record then cannot be formatted, and logging drops it — losing the
        line entirely instead of losing the secret. It has to survive, redacted.
        """
        settings, path = configured
        logging.getLogger("test.tmpl").debug(
            '>>> SEND {"opcode": 19, "payload": {"token": "%s"}}',
            settings.max_token)
        body = self._contents(path)
        assert settings.max_token not in body
        assert "opcode" in body, "the line was dropped instead of redacted"

    def test_every_registered_credential_is_covered(self, configured):
        settings, path = configured
        log = logging.getLogger("test.creds")
        for secret in (settings.max_token, settings.max_device_id,
                       settings.tg_bot_token):
            log.debug("value=%s", secret)
        body = self._contents(path)
        for secret in (settings.max_token, settings.max_device_id,
                       settings.tg_bot_token):
            assert secret not in body

    def test_a_signed_media_url_loses_its_query(self, configured):
        _settings, path = configured
        logging.getLogger("test.url").info(
            "downloading https://i.oneme.ru/p/abc?sig=secret-signature-value")
        body = self._contents(path)
        assert "secret-signature-value" not in body
        assert "i.oneme.ru/p/abc" in body

    def test_an_exception_traceback_is_redacted_too(self, configured):
        settings, path = configured
        log = logging.getLogger("test.exc")
        try:
            raise RuntimeError(f"auth failed for {settings.max_token}")
        except RuntimeError:
            log.exception("could not authorise")
        assert settings.max_token not in self._contents(path)

    def test_message_bodies_stay_out_by_default(self, configured):
        _settings, path = configured
        assert logging_utils.policy.log_message_text is False
        logging.getLogger("test.body").info(
            "text=%s", logging_utils.policy.preview("совершенно приватный текст"))
        assert "приватный" not in self._contents(path)
