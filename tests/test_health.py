"""Readiness accounting: an open socket is not the same as a working bridge."""

from __future__ import annotations

import time

import pytest

from app.errors import (
    AttachResult,
    Outcome,
    SendResult,
    coerce_retry_after,
    describe_exception,
    merge_outcomes,
)
from app.health import Counters, Health, LinkState, format_duration


class TestLinkState:
    def test_only_ready_is_ready(self):
        assert LinkState.READY.is_ready is True
        for state in LinkState:
            if state is not LinkState.READY:
                assert state.is_ready is False


class TestHealth:
    def test_starts_disconnected(self):
        assert Health().state is LinkState.DISCONNECTED
        assert Health().ready is False

    def test_becoming_ready_records_the_time(self):
        health = Health()
        health.set_state(LinkState.READY)
        assert health.ready is True
        assert health.last_auth_at is not None

    def test_an_incident_starts_on_disconnect(self):
        health = Health()
        health.set_state(LinkState.READY)
        health.set_state(LinkState.DISCONNECTED, "socket closed")
        assert health.incident_started_at is not None
        assert health.incident_age is not None

    def test_becoming_ready_clears_the_incident(self):
        health = Health()
        health.set_state(LinkState.DISCONNECTED)
        health.set_state(LinkState.READY)
        assert health.incident_started_at is None
        assert health.incident_age is None

    def test_repeating_a_state_does_not_reset_its_age(self):
        health = Health()
        health.set_state(LinkState.READY)
        first = health.state_since
        health.set_state(LinkState.READY)
        assert health.state_since == first

    def test_auth_required_counts_as_an_incident(self):
        health = Health()
        health.set_state(LinkState.AUTH_REQUIRED, "token rejected")
        assert health.incident_started_at is not None
        assert health.last_error == "token rejected"

    def test_delivery_is_counted(self):
        health = Health()
        health.note_delivery()
        assert health.counters.delivered == 1
        assert health.last_delivery_at is not None

    def test_snapshot_is_serialisable(self):
        health = Health()
        health.note_frame()
        health.set_state(LinkState.READY)
        snapshot = health.snapshot()
        assert snapshot["state"] == "ready"
        assert snapshot["ready"] is True
        assert snapshot["last_frame_age"] is not None
        assert "counters" in snapshot

    def test_snapshot_without_frames_reports_none(self):
        assert Health().snapshot()["last_frame_age"] is None

    def test_a_failing_queue_probe_does_not_break_the_snapshot(self):
        health = Health()

        def broken():
            raise RuntimeError("nope")

        health.queue_probe = broken
        assert health.snapshot()["queues"] == {"error": "probe failed"}


class TestCounters:
    def test_bump_creates_and_increments(self):
        counters = Counters()
        counters.bump("delivered")
        counters.bump("delivered", 4)
        assert counters.delivered == 5

    def test_as_dict_round_trips(self):
        counters = Counters()
        counters.bump("replays")
        assert counters.as_dict()["replays"] == 1


class TestFormatDuration:
    @pytest.mark.parametrize("seconds,expected", [
        (None, "—"), (5, "5с"), (90, "2м"), (7200, "2.0ч"), (172800, "2.0д"),
    ])
    def test_rendering(self, seconds, expected):
        assert format_duration(seconds) == expected

    def test_negative_is_clamped(self):
        assert format_duration(-10) == "0с"


class TestSendResult:
    def test_delivered_is_truthy(self):
        assert bool(SendResult.delivered([1])) is True

    def test_failures_are_falsy(self):
        assert bool(SendResult.transient("x")) is False
        assert bool(SendResult.permanent("x")) is False
        assert bool(SendResult.uncertain("x")) is False

    def test_only_transient_is_retryable(self):
        assert Outcome.TRANSIENT.retryable is True
        assert Outcome.UNCERTAIN.retryable is False
        assert Outcome.PERMANENT.retryable is False

    def test_retry_after_is_carried(self):
        assert SendResult.transient("x", retry_after=12).retry_after == 12


class TestMergeOutcomes:
    def test_all_delivered(self):
        assert merge_outcomes([Outcome.DELIVERED] * 3) is Outcome.DELIVERED

    def test_empty_is_delivered(self):
        assert merge_outcomes([]) is Outcome.DELIVERED

    def test_uncertain_wins_over_everything(self):
        """One part we cannot vouch for makes the whole job unsafe to replay."""
        assert merge_outcomes([Outcome.DELIVERED, Outcome.PERMANENT,
                               Outcome.TRANSIENT, Outcome.UNCERTAIN]) is Outcome.UNCERTAIN

    def test_transient_beats_permanent(self):
        assert merge_outcomes([Outcome.PERMANENT,
                               Outcome.TRANSIENT]) is Outcome.TRANSIENT

    def test_permanent_when_nothing_worse(self):
        assert merge_outcomes([Outcome.DELIVERED,
                               Outcome.PERMANENT]) is Outcome.PERMANENT


class TestAttachResult:
    def test_attachment_and_caption_are_tracked_separately(self):
        """A sticker delivers no caption; conflating them dropped text (M06)."""
        result = AttachResult(attachment_delivered=True, caption_delivered=False)
        assert result.ok is True
        assert result.caption_delivered is False

    def test_should_retry_covers_transient_and_uncertain(self):
        assert AttachResult(outcome=Outcome.TRANSIENT).should_retry is True
        assert AttachResult(outcome=Outcome.UNCERTAIN).should_retry is True
        assert AttachResult(outcome=Outcome.PERMANENT).should_retry is False


class TestCoerceRetryAfter:
    def test_plain_number(self):
        assert coerce_retry_after(5) == 5.0

    def test_timedelta(self):
        """PTB changed this type across supported versions (audit O02)."""
        from datetime import timedelta
        assert coerce_retry_after(timedelta(seconds=7)) == 7.0

    def test_none(self):
        assert coerce_retry_after(None) is None

    def test_garbage(self):
        assert coerce_retry_after("soon") is None


class TestDescribeException:
    def test_includes_type_and_message(self):
        assert describe_exception(ValueError("bad")) == "ValueError: bad"

    def test_bare_exception(self):
        assert describe_exception(RuntimeError()) == "RuntimeError"


class TestOutageMeasurement:
    """How long the link was down, still readable after it comes back.

    ``incident_started_at`` is cleared the instant the link goes ready, but the
    thing that has to warn "you may have missed messages" runs after that — the
    bridge has no catch-up of its own, so the gap is the only honest signal it
    can give (audit R07).
    """

    def test_a_clean_first_connect_reports_no_outage(self):
        health = Health()
        health.set_state(LinkState.CONNECTING)
        health.set_state(LinkState.HANDSHAKING)
        health.set_state(LinkState.AUTHENTICATING)
        health.set_state(LinkState.READY)
        assert health.last_outage_sec is None

    def test_an_outage_is_measured_when_it_ends(self):
        health = Health()
        health.set_state(LinkState.READY)
        health.set_state(LinkState.DISCONNECTED, "socket closed")
        health.incident_started_at = time.time() - 300
        health.set_state(LinkState.READY)
        assert health.last_outage_sec == pytest.approx(300, abs=5)
        assert health.incident_started_at is None

    def test_a_backoff_counts_as_part_of_the_same_outage(self):
        health = Health()
        health.set_state(LinkState.READY)
        health.set_state(LinkState.DISCONNECTED, "socket closed")
        started = health.incident_started_at
        health.set_state(LinkState.RECONNECT_BACKOFF, "retrying")
        assert health.incident_started_at == started

    def test_the_outage_reaches_the_snapshot(self):
        health = Health()
        health.set_state(LinkState.DISCONNECTED, "down")
        health.incident_started_at = time.time() - 120
        health.set_state(LinkState.READY)
        assert health.snapshot()["last_outage_sec"] == pytest.approx(120, abs=5)
