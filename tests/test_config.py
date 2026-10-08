"""Settings: everything is validated before a socket is opened."""

from __future__ import annotations

import os

import pytest

from app.config import ConfigError, load_settings

BASE_ENV = {
    "MAX_TOKEN": "token-value",
    "MAX_DEVICE_ID": "device-value",
    "TG_BOT_TOKEN": "123:abc",
    "TG_CHAT_ID": "-1001234567890",
    "TG_ALLOWED_USER_ID": "42",
}

_OWNED = [
    "MAX_TOKEN", "MAX_DEVICE_ID", "TG_BOT_TOKEN", "TG_CHAT_ID", "MAX_CHAT_IDS",
    "TG_ALLOWED_USER_ID", "TG_ADMIN_USER_ID", "TG_ALLOW_ANY_MEMBER",
    "TG_STATUS_CHAT_ID", "TG_PROXY", "TG_DROP_PENDING_UPDATES", "REPLY_ENABLED",
    "STATE_DIR", "LOG_DIR", "SPOOL_DIR", "SQLITE_SYNCHRONOUS", "LOG_LEVEL",
    "LOG_MESSAGE_TEXT", "DEBUG", "MEDIA_ALLOWED_HOSTS", "MEDIA_ALLOW_ANY_HOST",
    "MEDIA_ALLOW_PLAIN_HTTP", "MEDIA_MAX_BYTES", "SPOOL_BUDGET_BYTES",
    "DOWNLOAD_CONCURRENCY", "JOURNAL_RETENTION_DAYS", "DELIVERY_MAX_ATTEMPTS",
    "DELIVERY_RETRY_BASE_SEC", "MAX_AUDIO_PROBING", "EXECUTOR_THREADS",
    "SHUTDOWN_TIMEOUT_SEC", "MEDIA_TRUSTED_NETS",
]


@pytest.fixture
def env(monkeypatch, tmp_path):
    for name in _OWNED:
        monkeypatch.delenv(name, raising=False)
    # Keep load_dotenv() from picking up the developer's real .env.
    monkeypatch.setattr("app.config.load_dotenv", lambda *a, **k: None)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    return monkeypatch


class TestRequired:
    @pytest.mark.parametrize("name", ["MAX_TOKEN", "MAX_DEVICE_ID",
                                      "TG_BOT_TOKEN", "TG_CHAT_ID"])
    def test_missing_variable_is_named(self, env, name):
        env.delenv(name)
        with pytest.raises(ConfigError, match=name):
            load_settings()

    def test_whitespace_only_counts_as_missing(self, env):
        """A variable set to spaces used to pass and fail much later."""
        env.setenv("MAX_TOKEN", "   ")
        with pytest.raises(ConfigError, match="MAX_TOKEN"):
            load_settings()

    def test_values_are_stripped(self, env):
        env.setenv("MAX_TOKEN", "  abc  ")
        assert load_settings().max_token == "abc"


class TestAccessControl:
    def test_no_allowlist_and_no_opt_in_refuses_to_start(self, env):
        """Silently letting the whole group write as you is not a default (S03)."""
        env.delenv("TG_ALLOWED_USER_ID")
        with pytest.raises(ConfigError, match="Nobody is allowed"):
            load_settings()

    def test_explicit_open_mode_starts_with_a_warning(self, env):
        env.delenv("TG_ALLOWED_USER_ID")
        env.setenv("TG_ALLOW_ANY_MEMBER", "true")
        settings = load_settings()
        assert settings.allow_any_member is True
        assert any("TG_ALLOW_ANY_MEMBER" in note for note in settings.notes)

    def test_several_allowed_users(self, env):
        env.setenv("TG_ALLOWED_USER_ID", "42, 43 ,44")
        assert load_settings().tg_allowed_user_ids == frozenset({42, 43, 44})

    def test_zero_is_refused_rather_than_meaning_unrestricted(self, env):
        env.setenv("TG_ALLOWED_USER_ID", "0")
        with pytest.raises(ConfigError, match="not a valid Telegram user ID"):
            load_settings()

    def test_negative_user_id_is_refused(self, env):
        env.setenv("TG_ALLOWED_USER_ID", "-5")
        with pytest.raises(ConfigError):
            load_settings()

    def test_non_numeric_user_id_is_refused(self, env):
        env.setenv("TG_ALLOWED_USER_ID", "me")
        with pytest.raises(ConfigError, match="user IDs"):
            load_settings()

    def test_admins_are_parsed_separately(self, env):
        env.setenv("TG_ADMIN_USER_ID", "42")
        assert load_settings().tg_admin_user_ids == frozenset({42})


class TestChatIds:
    def test_default_is_no_filter(self, env):
        assert load_settings().inbound_chat_ids == ()

    def test_ids_are_parsed_and_normalised(self, env):
        env.setenv("MAX_CHAT_IDS", " -123 , -456 ")
        assert load_settings().inbound_chat_ids == ("-123", "-456")

    def test_a_typo_is_caught_at_startup(self, env):
        """This used to raise deep inside the MAX client, past all cleanup."""
        env.setenv("MAX_CHAT_IDS", "-123,oops")
        with pytest.raises(ConfigError, match="MAX_CHAT_IDS"):
            load_settings()

    def test_tg_chat_id_must_be_an_integer(self, env):
        env.setenv("TG_CHAT_ID", "not-a-number")
        with pytest.raises(ConfigError, match="TG_CHAT_ID"):
            load_settings()

    def test_a_positive_group_id_is_flagged(self, env):
        env.setenv("TG_CHAT_ID", "123456")
        assert any("negative" in note for note in load_settings().notes)


class TestFlags:
    @pytest.mark.parametrize("raw,expected", [
        ("true", True), ("TRUE", True), ("1", True), ("yes", True), ("on", True),
        ("false", False), ("0", False), ("no", False), ("", False),
    ])
    def test_boolean_parsing(self, env, raw, expected):
        env.setenv("DEBUG", raw)
        assert load_settings().debug is expected

    def test_booleans_tolerate_whitespace(self, env):
        env.setenv("DEBUG", "  true  ")
        assert load_settings().debug is True

    def test_an_unparseable_boolean_is_an_error(self, env):
        env.setenv("DEBUG", "maybe")
        with pytest.raises(ConfigError, match="DEBUG"):
            load_settings()

    def test_reply_is_enabled_by_default(self, env):
        assert load_settings().reply_enabled is True

    def test_pending_updates_are_kept_by_default(self, env):
        """Dropping them silently discarded everything typed while down (S01)."""
        assert load_settings().drop_pending_updates is False

    def test_dropping_updates_warns(self, env):
        env.setenv("TG_DROP_PENDING_UPDATES", "true")
        assert any("discarded" in n for n in load_settings().notes)


class TestLogging:
    def test_default_level(self, env):
        assert load_settings().log_level == "INFO"

    def test_level_is_upper_cased(self, env):
        env.setenv("LOG_LEVEL", "debug")
        assert load_settings().log_level == "DEBUG"

    def test_an_invalid_level_is_an_error_not_a_silent_fallback(self, env):
        env.setenv("LOG_LEVEL", "LOUD")
        with pytest.raises(ConfigError, match="LOG_LEVEL"):
            load_settings()

    def test_message_text_logging_is_off_by_default(self, env):
        assert load_settings().log_message_text is False

    def test_enabling_message_text_warns(self, env):
        env.setenv("LOG_MESSAGE_TEXT", "true")
        assert any("log file" in n for n in load_settings().notes)


class TestPaths:
    def test_directories_are_created(self, env, tmp_path):
        settings = load_settings()
        assert os.path.isdir(settings.state_dir)
        assert os.path.isdir(settings.spool_dir)
        assert settings.db_path.endswith("bridge.sqlite3")

    def test_an_unwritable_state_dir_is_refused(self, env, tmp_path):
        blocked = tmp_path / "blocked"
        blocked.mkdir(mode=0o500)
        env.setenv("STATE_DIR", str(blocked / "state"))
        with pytest.raises(ConfigError, match="STATE_DIR"):
            load_settings()


class TestProxy:
    def test_none_by_default(self, env):
        assert load_settings().tg_proxy is None

    @pytest.mark.parametrize("url", ["socks5://127.0.0.1:1080",
                                     "http://user:pass@host:3128"])
    def test_valid_proxies(self, env, url):
        env.setenv("TG_PROXY", url)
        assert load_settings().tg_proxy == url

    def test_a_malformed_proxy_is_refused(self, env):
        env.setenv("TG_PROXY", "just-a-host")
        with pytest.raises(ConfigError, match="TG_PROXY"):
            load_settings()


class TestMediaPolicy:
    def test_defaults_are_the_max_cdns(self, env):
        assert "oneme.ru" in load_settings().media_allowed_hosts

    def test_hosts_can_be_overridden(self, env):
        env.setenv("MEDIA_ALLOWED_HOSTS", "example.com, cdn.example.com")
        assert load_settings().media_allowed_hosts == ("example.com",
                                                       "cdn.example.com")

    def test_opening_up_hosts_warns(self, env):
        env.setenv("MEDIA_ALLOW_ANY_HOST", "true")
        assert any("MEDIA_ALLOW_ANY_HOST" in n for n in load_settings().notes)

    def test_no_trusted_nets_by_default(self, env):
        assert load_settings().media_trusted_nets == ()

    def test_trusted_nets_are_parsed_and_noted(self, env):
        env.setenv("MEDIA_TRUSTED_NETS", "198.18.0.0/15")
        settings = load_settings()
        assert [str(n) for n in settings.media_trusted_nets] == ["198.18.0.0/15"]
        assert any("MEDIA_TRUSTED_NETS" in n for n in settings.notes)

    @pytest.mark.parametrize("value", ["not-a-net", "127.0.0.0/8", "0.0.0.0/0",
                                       "169.254.0.0/16", "::1/128"])
    def test_dangerous_or_bad_trusted_nets_are_refused(self, env, value):
        env.setenv("MEDIA_TRUSTED_NETS", value)
        with pytest.raises(ConfigError, match="MEDIA_TRUSTED_NETS"):
            load_settings()

    def test_byte_limits_must_be_positive(self, env):
        env.setenv("MEDIA_MAX_BYTES", "0")
        with pytest.raises(ConfigError, match="MEDIA_MAX_BYTES"):
            load_settings()


class TestSqlite:
    def test_full_by_default(self, env):
        assert load_settings().sqlite_synchronous == "FULL"

    def test_relaxed_durability_warns(self, env):
        env.setenv("SQLITE_SYNCHRONOUS", "normal")
        settings = load_settings()
        assert settings.sqlite_synchronous == "NORMAL"
        assert any("power loss" in n for n in settings.notes)

    def test_an_invalid_mode_is_refused(self, env):
        env.setenv("SQLITE_SYNCHRONOUS", "SOMETIMES")
        with pytest.raises(ConfigError):
            load_settings()


class TestDeliveryTuning:
    def test_defaults(self, env):
        settings = load_settings()
        assert settings.max_attempts == 5
        assert settings.retry_base_delay == 5.0
        assert settings.download_concurrency == 4

    def test_attempts_must_be_positive(self, env):
        env.setenv("DELIVERY_MAX_ATTEMPTS", "0")
        with pytest.raises(ConfigError):
            load_settings()

    def test_a_non_numeric_delay_is_refused(self, env):
        env.setenv("DELIVERY_RETRY_BASE_SEC", "soon")
        with pytest.raises(ConfigError, match="DELIVERY_RETRY_BASE_SEC"):
            load_settings()

    def test_audio_probing_is_off_by_default(self, env):
        assert load_settings().enable_audio_probing is False
