"""Forgetting-risk estimation.

"What am I likely forgetting that's still important?" — score memory nodes by
``importance × staleness`` and surface the high-risk ones, each grounded to L0.

- importance: PageRank ``centrality`` (graph step), with a support-based fallback so the
  command is useful before ``build-memory`` computes centrality.
- staleness: ``1 - 0.5 ** (days_since_last_touch / halflife)`` where last_touch unifies usage
  recency (node ``open`` events) and content age (node times / grounded evidence). Rehearsing a
  node (recent use) lowers its staleness — and thus its risk.
- confidence: from L0 grounding support; low-support nodes are surfaced with low confidence and
  nodes below ``min_support`` are excluded (grounding guarantee).

Read-only and `now`-injectable (deterministic). Nothing here writes; usage stays separable.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from crossmodalrag.memory.store import resolve_to_evidence
from crossmodalrag.usage.store import usage_summaries

# How much support (distinct grounded L0 chunks) counts as "fully" important / confident.
IMPORTANCE_SUPPORT_FULL = 3.0
CONFIDENCE_SUPPORT_FULL = 3.0

_SECONDS_PER_DAY = 86400.0

LEVEL_NAMES: dict[str, tuple[int, ...]] = {
    "event": (1,),
    "episode": (2,),
    "concept": (3,),
    "all": (1, 2, 3),
}


@dataclass(frozen=True)
class ForgettingRisk:
    node_id: int
    level: int
    node_type: str
    title: str | None
    importance: float
    staleness: float
    risk: float
    confidence: float
    support: int
    last_touch: str | None
    evidence_source_uris: list[str]


def forgetting_risk_to_dict(item: ForgettingRisk) -> dict:
    """Stable JSON contract for `mem forgetting --json`. Keep field names backward-compatible."""
    return {
        "node_id": item.node_id,
        "level": item.level,
        "node_type": item.node_type,
        "title": item.title,
        "risk": item.risk,
        "importance": item.importance,
        "staleness": item.staleness,
        "confidence": item.confidence,
        "support": item.support,
        "last_touch": item.last_touch,
        "evidence_source_uris": list(item.evidence_source_uris),
    }


def compute_forgetting_risk(
    conn: sqlite3.Connection,
    *,
    now: datetime,
    halflife_days: float,
    levels: tuple[int, ...] = (3,),
    min_support: int = 1,
    top: int | None = None,
) -> list[ForgettingRisk]:
    """Rank memory nodes in ``levels`` by forgetting risk (importance × staleness)."""
    placeholders = ",".join("?" for _ in levels)
    rows = conn.execute(
        f"""
        SELECT id, level, node_type, title, time_start, time_end, centrality
        FROM memory_nodes
        WHERE level IN ({placeholders})
        ORDER BY id ASC
        """,
        tuple(levels),
    ).fetchall()

    node_usage = {
        target_id: summary.last_event_at
        for (kind, target_id), summary in usage_summaries(
            conn, now=now, halflife_days=halflife_days
        ).items()
        if kind == "node"
    }

    items: list[ForgettingRisk] = []
    for row in rows:
        node_id = int(row["id"])
        level = int(row["level"])
        chunk_ids = resolve_to_evidence(conn, level, node_id)
        support = len(chunk_ids)
        if support < min_support:
            continue  # ungrounded / weakly-grounded nodes never surface

        content_time = _max_source_timestamp(conn, chunk_ids)
        last_touch = _latest(
            node_usage.get(node_id),
            row["time_end"],
            row["time_start"],
            content_time,
        )
        staleness = _staleness(last_touch, now=now, halflife_days=halflife_days)

        centrality = row["centrality"]
        importance = (
            float(centrality)
            if centrality is not None
            else min(1.0, support / IMPORTANCE_SUPPORT_FULL)
        )
        confidence = min(1.0, support / CONFIDENCE_SUPPORT_FULL)

        items.append(
            ForgettingRisk(
                node_id=node_id,
                level=level,
                node_type=str(row["node_type"]),
                title=row["title"],
                importance=importance,
                staleness=staleness,
                risk=importance * staleness,
                confidence=confidence,
                support=support,
                last_touch=last_touch,
                evidence_source_uris=_source_uris(conn, chunk_ids, limit=3),
            )
        )

    items.sort(key=lambda r: (-r.risk, -r.confidence, r.node_id))
    return items[:top] if top is not None else items


def _staleness(last_touch: str | None, *, now: datetime, halflife_days: float) -> float:
    if last_touch is None or halflife_days <= 0:
        return 1.0  # unknown age / no rehearsal -> treat as maximally stale
    dt = _parse(last_touch)
    if dt is None:
        return 1.0
    days = max((now - dt).total_seconds() / _SECONDS_PER_DAY, 0.0)
    return 1.0 - (0.5 ** (days / halflife_days))


def _latest(*timestamps: str | None) -> str | None:
    parsed = [(ts, _parse(ts)) for ts in timestamps if ts]
    valid = [(ts, dt) for ts, dt in parsed if dt is not None]
    if not valid:
        return None
    return max(valid, key=lambda pair: pair[1])[0]


def _max_source_timestamp(conn: sqlite3.Connection, chunk_ids: list[int]) -> str | None:
    if not chunk_ids:
        return None
    placeholders = ",".join("?" for _ in chunk_ids)
    row = conn.execute(
        f"""
        SELECT MAX(s.timestamp) AS ts
        FROM evidence_chunks c JOIN sources s ON s.id = c.source_id
        WHERE c.id IN ({placeholders})
        """,
        tuple(chunk_ids),
    ).fetchone()
    return row["ts"] if row is not None else None


def _source_uris(conn: sqlite3.Connection, chunk_ids: list[int], *, limit: int) -> list[str]:
    if not chunk_ids:
        return []
    placeholders = ",".join("?" for _ in chunk_ids)
    rows = conn.execute(
        f"""
        SELECT DISTINCT s.source_uri AS uri
        FROM evidence_chunks c JOIN sources s ON s.id = c.source_id
        WHERE c.id IN ({placeholders})
        ORDER BY s.source_uri ASC
        """,
        tuple(chunk_ids),
    ).fetchall()
    return [str(r["uri"]) for r in rows[:limit]]


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
