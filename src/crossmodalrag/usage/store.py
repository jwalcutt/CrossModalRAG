"""Data access for the append-only usage signal.

Append-only inserts + read aggregation. Usage is a *separable* layer: it is never part
of any content/derivation fingerprint, and ``clear_usage_events`` returns ranking inputs
to the exact baseline. Endpoints are polymorphic (chunk/node) with no DB FK,
mirroring ``memory_edges``.
"""

from __future__ import annotations

import sqlite3

from crossmodalrag.usage.strength import (
    UsageEvent,
    UsageSummary,
    default_weight,
    summarize,
)


def record_usage_event(
    conn: sqlite3.Connection,
    target_kind: str,
    target_id: int,
    event_type: str,
    *,
    event_at: str,
    weight: float | None = None,
) -> int:
    """Append a usage event. ``weight`` defaults from EVENT_WEIGHTS; ``event_at`` is explicit."""
    w = default_weight(event_type) if weight is None else float(weight)
    cur = conn.execute(
        """
        INSERT INTO usage_events (target_kind, target_id, event_type, weight, event_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (target_kind, int(target_id), event_type, w, event_at),
    )
    return int(cur.lastrowid)


def list_usage_events(
    conn: sqlite3.Connection,
    *,
    target_kind: str | None = None,
    target_id: int | None = None,
) -> list[UsageEvent]:
    sql = "SELECT id, target_kind, target_id, event_type, weight, event_at FROM usage_events"
    clauses: list[str] = []
    params: list[object] = []
    if target_kind is not None:
        clauses.append("target_kind = ?")
        params.append(target_kind)
    if target_id is not None:
        clauses.append("target_id = ?")
        params.append(int(target_id))
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id ASC"
    return [_row_to_event(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def usage_summaries(
    conn: sqlite3.Connection, *, now, halflife_days: float
) -> dict[tuple[str, int], UsageSummary]:
    """Bulk per-target rehearsal summaries. The read API the step-2 scorer will consume."""
    grouped: dict[tuple[str, int], list[UsageEvent]] = {}
    for event in list_usage_events(conn):
        grouped.setdefault((event.target_kind, event.target_id), []).append(event)
    out: dict[tuple[str, int], UsageSummary] = {}
    for key, events in grouped.items():
        summary = summarize(events, now=now, halflife_days=halflife_days)
        if summary is not None:
            out[key] = summary
    return out


def clear_usage_events(
    conn: sqlite3.Connection,
    *,
    target_kind: str | None = None,
    target_ids: list[int] | None = None,
) -> int:
    """Delete usage events (optionally scoped). Returns rows deleted. Commits."""
    sql = "DELETE FROM usage_events"
    clauses: list[str] = []
    params: list[object] = []
    if target_kind is not None:
        clauses.append("target_kind = ?")
        params.append(target_kind)
    if target_ids is not None:
        if not target_ids:
            return 0
        placeholders = ",".join("?" for _ in target_ids)
        clauses.append(f"target_id IN ({placeholders})")
        params.extend(int(t) for t in target_ids)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    deleted = conn.execute(sql, tuple(params)).rowcount
    conn.commit()
    return int(deleted)


def _row_to_event(row: sqlite3.Row) -> UsageEvent:
    return UsageEvent(
        id=int(row["id"]),
        target_kind=str(row["target_kind"]),
        target_id=int(row["target_id"]),
        event_type=str(row["event_type"]),
        weight=float(row["weight"]),
        event_at=str(row["event_at"]),
    )
