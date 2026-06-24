"""Usage signal: an additive, separable interaction-history layer.

Never part of any content/derivation fingerprint — clearing usage restores the exact
baseline. Provides the store + the pure rehearsal-strength model only; ranking integration
and real interaction tracking come later.
"""

from __future__ import annotations

from crossmodalrag.usage.store import (
    clear_usage_events,
    list_usage_events,
    record_usage_event,
    usage_summaries,
)
from crossmodalrag.usage.strength import (
    EVENT_TYPES,
    EVENT_WEIGHTS,
    TARGET_KINDS,
    UsageEvent,
    UsageSummary,
    default_weight,
    rehearsal_strength,
    summarize,
)

__all__ = [
    "EVENT_TYPES",
    "EVENT_WEIGHTS",
    "TARGET_KINDS",
    "UsageEvent",
    "UsageSummary",
    "clear_usage_events",
    "default_weight",
    "list_usage_events",
    "record_usage_event",
    "rehearsal_strength",
    "summarize",
    "usage_summaries",
]
