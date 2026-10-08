"""The container probe: readiness, not "the process exists"."""

from __future__ import annotations

import json
import time

from app.healthcheck import (
    MAX_BACKLOG_AGE_SEC,
    MAX_SNAPSHOT_AGE_SEC,
    evaluate,
    health_path,
    write_snapshot,
)


def fresh(**overrides):
    snapshot = {"written_at": time.time(), "ready": True, "state": "ready"}
    snapshot.update(overrides)
    return snapshot


class TestEvaluate:
    def test_a_ready_bridge_passes(self):
        ok, reason = evaluate(fresh())
        assert ok is True
        assert "ready" in reason

    def test_a_stale_snapshot_fails(self):
        """Nothing writing the snapshot means the event loop is not turning."""
        ok, reason = evaluate(fresh(written_at=time.time() - MAX_SNAPSHOT_AGE_SEC - 1))
        assert ok is False
        assert "stuck" in reason

    def test_a_snapshot_without_a_timestamp_fails(self):
        ok, reason = evaluate({"ready": True})
        assert ok is False
        assert "timestamp" in reason

    def test_an_unauthorised_link_is_not_ready(self):
        """An open socket in a reconnect loop used to read as perfectly healthy."""
        ok, reason = evaluate(fresh(ready=False, state="auth_required"))
        assert ok is False
        assert "auth_required" in reason

    def test_a_reconnecting_link_is_not_ready(self):
        ok, _reason = evaluate(fresh(ready=False, state="reconnect_backoff"))
        assert ok is False

    def test_a_stalled_backlog_fails_even_with_a_live_link(self):
        """A working socket does not mean anything is being delivered."""
        ok, reason = evaluate(fresh(oldest_pending_age=MAX_BACKLOG_AGE_SEC + 1))
        assert ok is False
        assert "undelivered" in reason

    def test_a_small_backlog_is_fine(self):
        assert evaluate(fresh(oldest_pending_age=5))[0] is True

    def test_no_backlog_field(self):
        assert evaluate(fresh(oldest_pending_age=None))[0] is True


class TestSnapshotFile:
    def test_write_and_read_back(self, tmp_path):
        path = str(tmp_path / "health.json")
        write_snapshot(path, {"state": "ready", "ready": True})
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        assert data["ready"] is True
        assert isinstance(data["written_at"], float)

    def test_write_is_atomic(self, tmp_path):
        path = str(tmp_path / "health.json")
        write_snapshot(path, {"a": 1})
        write_snapshot(path, {"a": 2})
        with open(path, encoding="utf-8") as handle:
            assert json.load(handle)["a"] == 2
        assert not (tmp_path / "health.json.tmp").exists()

    def test_non_serialisable_values_do_not_break_it(self, tmp_path):
        path = str(tmp_path / "health.json")
        write_snapshot(path, {"state": object()})
        with open(path, encoding="utf-8") as handle:
            assert isinstance(json.load(handle)["state"], str)

    def test_path_follows_the_state_dir(self, monkeypatch):
        monkeypatch.setenv("STATE_DIR", "/srv/state")
        assert health_path() == "/srv/state/health.json"

    def test_explicit_dir_wins(self):
        assert health_path("/tmp/x") == "/tmp/x/health.json"
