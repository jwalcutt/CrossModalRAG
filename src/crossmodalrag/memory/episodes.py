from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from crossmodalrag.config import get_episode_gap_seconds
from crossmodalrag.memory.store import (
    MemoryNode,
    add_edge,
    delete_node,
    insert_node,
    list_nodes,
)

EPISODE_GROUPING_VERSION = "l2-episodes-v1"
EPISODE_LEVEL = 2
EVENT_LEVEL = 1
MAX_TITLE_EVENTS = 8


@dataclass(frozen=True)
class EpisodeResult:
    episodes_created: int
    episodes_kept: int
    episodes_deleted: int
    events_grouped: int


def build_episodes(conn: sqlite3.Connection, *, gap_seconds: int | None = None) -> EpisodeResult:
    """Group L1 events into L2 episodes (project + time-gap sessions), idempotently.

    Reconciles by fingerprint: episodes whose exact member-event set is unchanged
    are kept untouched (so re-running on an unchanged L1 layer is a no-op and
    `contains` edges never dangle); stale episodes are deleted and new ones created.
    Deterministic and LLM-free.
    """
    if gap_seconds is None:
        gap_seconds = get_episode_gap_seconds()

    events = list_nodes(conn, level=EVENT_LEVEL, node_type="event")
    groups = _group_events(events, gap_seconds)

    desired: dict[str, tuple] = {}
    events_grouped = 0
    for project_key, members in groups:
        member_ids = sorted(node.id for node in members)
        fingerprint = _episode_fingerprint(project_key, member_ids, gap_seconds)
        desired[fingerprint] = (project_key, members, member_ids)
        events_grouped += len(members)

    existing = {
        node.derivation_fingerprint: node.id
        for node in list_nodes(conn, level=EPISODE_LEVEL, node_type="episode")
        if node.derivation_fingerprint is not None
    }

    created = 0
    kept = 0
    for fingerprint, (project_key, members, member_ids) in desired.items():
        if fingerprint in existing:
            kept += 1
            continue
        time_start, time_end = _time_span(members)
        node_id = insert_node(
            conn,
            level=EPISODE_LEVEL,
            node_type="episode",
            title=_episode_title(project_key, members, time_start, time_end),
            content=_episode_content(members),
            time_start=time_start,
            time_end=time_end,
            derivation_fingerprint=fingerprint,
            prompt_version=EPISODE_GROUPING_VERSION,
            metadata=json.dumps({"project_key": project_key, "event_count": len(members)}),
        )
        for member_id in member_ids:
            add_edge(conn, EPISODE_LEVEL, node_id, EVENT_LEVEL, member_id, "contains")
        created += 1

    deleted = 0
    for fingerprint, node_id in existing.items():
        if fingerprint not in desired:
            delete_node(conn, node_id)
            deleted += 1

    conn.commit()
    return EpisodeResult(
        episodes_created=created,
        episodes_kept=kept,
        episodes_deleted=deleted,
        events_grouped=events_grouped,
    )


def _group_events(events: list[MemoryNode], gap_seconds: int) -> list[tuple[str, list[MemoryNode]]]:
    """Bucket events by project key, then sessionize each bucket by time gap."""
    buckets: dict[str, list[MemoryNode]] = {}
    for event in events:
        buckets.setdefault(_event_project_key(event), []).append(event)

    groups: list[tuple[str, list[MemoryNode]]] = []
    for project_key in sorted(buckets):
        dated: list[tuple[datetime, MemoryNode]] = []
        undated: list[MemoryNode] = []
        for event in buckets[project_key]:
            dt = _parse_ts(event.time_start)
            if dt is None:
                undated.append(event)
            else:
                dated.append((dt, event))

        dated.sort(key=lambda pair: (pair[0], pair[1].id))
        current: list[MemoryNode] = []
        prev_dt: datetime | None = None
        for dt, event in dated:
            if prev_dt is not None and (dt - prev_dt).total_seconds() > gap_seconds:
                groups.append((project_key, current))
                current = []
            current.append(event)
            prev_dt = dt
        if current:
            groups.append((project_key, current))

        if undated:
            groups.append((project_key, sorted(undated, key=lambda n: n.id)))

    return groups


def _event_project_key(event: MemoryNode) -> str:
    source_type = ""
    source_uri = ""
    if event.metadata_json:
        try:
            meta = json.loads(event.metadata_json)
            source_type = str(meta.get("source_type", ""))
            source_uri = str(meta.get("source_uri", ""))
        except json.JSONDecodeError:
            pass
    return _project_key(source_type, source_uri)


def _project_key(source_type: str, source_uri: str) -> str:
    if not source_uri:
        return source_type or "unknown"
    if source_type == "git_commit":
        return source_uri.rsplit("@", 1)[0]
    # File-path modalities (notes, PDFs, images) group by containing directory, so a
    # folder of related sources — including a mix of notes/PDFs/screenshots — forms one
    # cross-modal episode rather than every PDF/image collapsing into a single project.
    if source_type in ("note", "pdf", "image"):
        return os.path.dirname(source_uri) or source_uri
    return source_type or source_uri


def _parse_ts(timestamp: str | None) -> datetime | None:
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _time_span(members: list[MemoryNode]) -> tuple[str | None, str | None]:
    times = [m.time_start for m in members if _parse_ts(m.time_start) is not None]
    if not times:
        return None, None
    return min(times), max(times)


def _episode_title(
    project_key: str,
    members: list[MemoryNode],
    time_start: str | None,
    time_end: str | None,
) -> str:
    label = os.path.basename(project_key.rstrip("/")) or project_key
    n = len(members)
    plural = "event" if n == 1 else "events"
    if time_start is None:
        return f"{label}: {n} {plural}, undated"
    start_date = time_start[:10]
    end_date = time_end[:10] if time_end else start_date
    span = start_date if start_date == end_date else f"{start_date}..{end_date}"
    return f"{label}: {n} {plural}, {span}"


def _episode_content(members: list[MemoryNode]) -> str:
    titles = [str(m.title) for m in members if m.title]
    shown = titles[:MAX_TITLE_EVENTS]
    text = "; ".join(shown)
    if len(titles) > MAX_TITLE_EVENTS:
        text += f"; … (+{len(titles) - MAX_TITLE_EVENTS} more)"
    return text


def _episode_fingerprint(project_key: str, member_ids: list[int], gap_seconds: int) -> str:
    payload = (
        f"{EPISODE_GROUPING_VERSION}\x1f{gap_seconds}\x1f{project_key}\x1f"
        f"{','.join(str(i) for i in member_ids)}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
