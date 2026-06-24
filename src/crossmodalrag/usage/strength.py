"""Pure rehearsal-strength model — no sqlite, no wall-clock.

A memory item's *rehearsal strength* is a spaced-repetition-style signal: each usage
event contributes its weight, decayed by a half-life as it ages, so frequency (more
events) and recency (recent events count more) both fall out of one sum. The function
is pure and ``now``-injectable (mirroring ``retrieve/lexical.py::recency_score``), so
scores are deterministic and frozen-clock-testable.

This module is intentionally storage-agnostic; ``usage/store.py`` loads events and
calls in here. Nothing here is part of any content/derivation fingerprint.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# Default per-event-type weights. The ``weight`` column lets a caller override per event.
EVENT_WEIGHTS: dict[str, float] = {
    "retrieval_hit": 1.0,
    "open": 2.0,
    "accepted_answer": 3.0,
}

# Event-type vocabulary (kept here so store + callers share one source of truth).
EVENT_TYPES: frozenset[str] = frozenset(EVENT_WEIGHTS)
TARGET_KINDS: frozenset[str] = frozenset({"chunk", "node"})

_SECONDS_PER_DAY = 86400.0


@dataclass(frozen=True)
class UsageEvent:
    target_kind: str
    target_id: int
    event_type: str
    weight: float
    event_at: str  # ISO8601
    id: int | None = None


@dataclass(frozen=True)
class UsageSummary:
    target_kind: str
    target_id: int
    count: int
    last_event_at: str | None
    strength: float


def default_weight(event_type: str) -> float:
    return EVENT_WEIGHTS.get(event_type, 1.0)


def normalize_strength(strength: float, *, saturation: float) -> float:
    """Map an unbounded rehearsal strength into [0, 1) for blending into a score.

    Saturating (``strength / (strength + saturation)``): 0 -> 0, diminishing returns as
    strength grows, deterministic and pool-independent. ``saturation`` sets how much
    strength counts as "well rehearsed" (the half-saturation point).
    """
    if strength <= 0 or saturation <= 0:
        return 0.0
    return strength / (strength + saturation)


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _age_days(event_at: str, now: datetime) -> float | None:
    dt = _parse(event_at)
    if dt is None:
        return None
    age = (now - dt).total_seconds() / _SECONDS_PER_DAY
    return age if age > 0 else 0.0  # future events clamp to age 0 (no future-dated boost)


def rehearsal_strength(
    events: list[UsageEvent], *, now: datetime, halflife_days: float
) -> float:
    """Sum of event weights, each halving every ``halflife_days`` as it ages.

    ``strength = Σ weight_i · 0.5 ** (age_days_i / H)``. Empty -> 0.0. Events with an
    unparseable ``event_at`` are skipped (defensive, never raise in a scoring path).
    """
    if not events or halflife_days <= 0:
        return 0.0
    total = 0.0
    for event in events:
        age = _age_days(event.event_at, now)
        if age is None:
            continue
        total += float(event.weight) * (0.5 ** (age / halflife_days))
    return total


def summarize(
    events: list[UsageEvent], *, now: datetime, halflife_days: float
) -> UsageSummary | None:
    """Aggregate one target's events into a UsageSummary (None if no events)."""
    if not events:
        return None
    first = events[0]
    last_at = max((e.event_at for e in events if _parse(e.event_at) is not None), default=None)
    return UsageSummary(
        target_kind=first.target_kind,
        target_id=first.target_id,
        count=len(events),
        last_event_at=last_at,
        strength=rehearsal_strength(events, now=now, halflife_days=halflife_days),
    )
