from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crossmodalrag.usage.strength import (
    EVENT_WEIGHTS,
    UsageEvent,
    default_weight,
    rehearsal_strength,
    summarize,
)

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _ev(event_type: str, event_at: str, weight: float | None = None) -> UsageEvent:
    return UsageEvent(
        target_kind="chunk",
        target_id=1,
        event_type=event_type,
        weight=default_weight(event_type) if weight is None else weight,
        event_at=event_at,
    )


def test_empty_events_have_zero_strength():
    assert rehearsal_strength([], now=NOW, halflife_days=30) == 0.0


def test_event_at_one_halflife_ago_decays_to_half_weight():
    # 30 days before NOW with halflife 30 -> 0.5 ** 1 = 0.5 of the weight.
    e = _ev("retrieval_hit", "2026-06-01T00:00:00+00:00")  # exactly 30 days before NOW
    strength = rehearsal_strength([e], now=NOW, halflife_days=30)
    assert strength == pytest.approx(0.5, abs=1e-6)


def test_fresh_event_keeps_full_weight():
    e = _ev("accepted_answer", "2026-07-01T00:00:00+00:00")  # age 0
    assert rehearsal_strength([e], now=NOW, halflife_days=30) == pytest.approx(
        EVENT_WEIGHTS["accepted_answer"]
    )


def test_frequency_adds_up():
    events = [
        _ev("retrieval_hit", "2026-07-01T00:00:00+00:00"),
        _ev("retrieval_hit", "2026-07-01T00:00:00+00:00"),
    ]
    assert rehearsal_strength(events, now=NOW, halflife_days=30) == pytest.approx(2.0)


def test_future_event_is_clamped_to_age_zero():
    e = _ev("retrieval_hit", "2027-01-01T00:00:00+00:00")  # after NOW
    assert rehearsal_strength([e], now=NOW, halflife_days=30) == pytest.approx(1.0)


def test_event_weights_are_applied():
    assert default_weight("accepted_answer") == 3.0
    assert default_weight("open") == 2.0
    assert default_weight("unknown") == 1.0


def test_deterministic_under_frozen_now():
    events = [
        _ev("retrieval_hit", "2026-05-01T00:00:00+00:00"),
        _ev("open", "2026-06-15T00:00:00+00:00"),
    ]
    a = rehearsal_strength(events, now=NOW, halflife_days=30)
    b = rehearsal_strength(events, now=NOW, halflife_days=30)
    assert a == b


def test_unparseable_event_at_is_skipped_not_raised():
    events = [_ev("retrieval_hit", "not-a-date"), _ev("retrieval_hit", "2026-07-01T00:00:00+00:00")]
    assert rehearsal_strength(events, now=NOW, halflife_days=30) == pytest.approx(1.0)


def test_summarize_reports_count_last_and_strength():
    events = [
        _ev("retrieval_hit", "2026-06-01T00:00:00+00:00"),
        _ev("open", "2026-06-20T00:00:00+00:00"),
    ]
    summary = summarize(events, now=NOW, halflife_days=30)
    assert summary is not None
    assert summary.count == 2
    assert summary.last_event_at == "2026-06-20T00:00:00+00:00"
    assert summary.strength == pytest.approx(
        rehearsal_strength(events, now=NOW, halflife_days=30)
    )


def test_summarize_empty_is_none():
    assert summarize([], now=NOW, halflife_days=30) is None
