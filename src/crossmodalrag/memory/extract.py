from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from crossmodalrag.generate.provider import LLMProvider
from crossmodalrag.memory.store import (
    EVIDENCE_LEVEL,
    add_edge,
    delete_node,
    insert_node,
    list_nodes,
)

EVENT_PROMPT_VERSION = "l1-events-v1"
EVENT_LEVEL = 1
MAX_EVENTS_PER_SOURCE = 10
MAX_SOURCE_CHARS = 8000

EVENT_SYSTEM_PROMPT = (
    "You extract atomic events from a single source document (a note or a git commit).\n"
    "An atomic event is the smallest meaningful 'happened item': one decision, learning, fix, "
    "task, or change. Extract only events that are explicitly grounded in the provided text; "
    "do not invent or infer beyond it.\n"
    "Respond with ONLY a JSON array (no prose, no code fences) of objects with keys:\n"
    '  "title": a concise event title (<= ~12 words)\n'
    '  "summary": one sentence describing the event\n'
    "Return at most 10 events. If the text contains no meaningful event, return []."
)


@dataclass(frozen=True)
class ExtractionResult:
    sources_processed: int
    sources_skipped: int
    events_created: int
    parse_failures: int


def extract_pending_sources(
    conn: sqlite3.Connection,
    provider: LLMProvider,
    *,
    prompt_version: str = EVENT_PROMPT_VERSION,
    limit: int | None = None,
) -> ExtractionResult:
    """Extract L1 events for sources whose events are missing or stale.

    Resumable and incremental: a source is processed only when its derivation
    fingerprint (content + model + prompt_version) differs from what is already
    stored, so re-running on unchanged data is a no-op. ``limit`` caps the number
    of sources *processed* this run (skips do not count), enabling staged passes.
    """
    processed = 0
    skipped = 0
    events_created = 0
    parse_failures = 0

    rows = conn.execute("SELECT id FROM sources ORDER BY id ASC").fetchall()
    for row in rows:
        if limit is not None and processed >= limit:
            break
        source_id = int(row["id"])
        text = _source_text(conn, source_id)
        fingerprint = _fingerprint(provider.name, prompt_version, text)
        if _is_up_to_date(conn, source_id, fingerprint):
            skipped += 1
            continue

        created, failed = _extract_for_source(
            conn,
            provider,
            source_id=source_id,
            source_text=text,
            fingerprint=fingerprint,
            prompt_version=prompt_version,
        )
        events_created += created
        parse_failures += failed
        processed += 1
        conn.commit()

    return ExtractionResult(
        sources_processed=processed,
        sources_skipped=skipped,
        events_created=events_created,
        parse_failures=parse_failures,
    )


def extract_events_for_source(
    conn: sqlite3.Connection,
    provider: LLMProvider,
    source_id: int,
    *,
    prompt_version: str = EVENT_PROMPT_VERSION,
) -> int:
    """Extract events for one source, skipping if already up to date. Returns events created."""
    text = _source_text(conn, source_id)
    fingerprint = _fingerprint(provider.name, prompt_version, text)
    if _is_up_to_date(conn, source_id, fingerprint):
        return 0
    created, _ = _extract_for_source(
        conn,
        provider,
        source_id=source_id,
        source_text=text,
        fingerprint=fingerprint,
        prompt_version=prompt_version,
    )
    conn.commit()
    return created


def _extract_for_source(
    conn: sqlite3.Connection,
    provider: LLMProvider,
    *,
    source_id: int,
    source_text: str,
    fingerprint: str,
    prompt_version: str,
) -> tuple[int, int]:
    """(Re)derive events for a source. Returns (events_created, parse_failures)."""
    # Clear any stale events for this source before re-deriving.
    for node in _events_for_source(conn, source_id):
        delete_node(conn, node.id)

    if not source_text.strip():
        return 0, 0

    chunk_ids = _source_chunk_ids(conn, source_id)
    if not chunk_ids:
        return 0, 0

    source_meta = conn.execute(
        "SELECT source_uri, source_type, timestamp FROM sources WHERE id = ?",
        (source_id,),
    ).fetchone()

    raw = provider.generate(_build_prompt(source_text), system=EVENT_SYSTEM_PROMPT)
    events = _parse_events(raw)
    if events is None:
        # Unparseable: do not record a derivation so the source is retried next run.
        return 0, 1

    # Record the derivation (even for zero events) so an unchanged source is skipped next run.
    _record_derivation(conn, source_id, fingerprint, provider.name, prompt_version)

    created = 0
    metadata = json.dumps(
        {
            "source_id": source_id,
            "source_uri": source_meta["source_uri"],
            "source_type": source_meta["source_type"],
        }
    )
    for event in events:
        node_id = insert_node(
            conn,
            level=1,
            node_type="event",
            title=event["title"],
            content=event.get("summary"),
            time_start=source_meta["timestamp"],
            time_end=source_meta["timestamp"],
            derivation_fingerprint=fingerprint,
            model=provider.name,
            prompt_version=prompt_version,
            metadata=metadata,
        )
        for chunk_id in chunk_ids:
            add_edge(conn, 1, node_id, EVIDENCE_LEVEL, chunk_id, "derived_from")
        created += 1

    return created, 0


def _build_prompt(source_text: str) -> str:
    return f"Source document:\n{source_text}\n\nExtract the atomic events as a JSON array:"


def _source_text(conn: sqlite3.Connection, source_id: int) -> str:
    rows = conn.execute(
        "SELECT chunk_text FROM evidence_chunks WHERE source_id = ? ORDER BY chunk_index ASC",
        (source_id,),
    ).fetchall()
    text = "\n\n".join(str(r["chunk_text"]) for r in rows)
    return text[:MAX_SOURCE_CHARS]


def _source_chunk_ids(conn: sqlite3.Connection, source_id: int) -> list[int]:
    rows = conn.execute(
        "SELECT id FROM evidence_chunks WHERE source_id = ? ORDER BY chunk_index ASC",
        (source_id,),
    ).fetchall()
    return [int(r["id"]) for r in rows]


def _events_for_source(conn: sqlite3.Connection, source_id: int) -> list:
    out = []
    for node in list_nodes(conn, level=1, node_type="event"):
        if not node.metadata_json:
            continue
        try:
            meta = json.loads(node.metadata_json)
        except json.JSONDecodeError:
            continue
        if meta.get("source_id") == source_id:
            out.append(node)
    return out


def _is_up_to_date(conn: sqlite3.Connection, source_id: int, fingerprint: str) -> bool:
    row = conn.execute(
        "SELECT fingerprint FROM memory_derivations WHERE source_id = ? AND level = ?",
        (source_id, EVENT_LEVEL),
    ).fetchone()
    return row is not None and str(row["fingerprint"]) == fingerprint


def _record_derivation(
    conn: sqlite3.Connection,
    source_id: int,
    fingerprint: str,
    model: str,
    prompt_version: str,
) -> None:
    conn.execute(
        """
        INSERT INTO memory_derivations (source_id, level, fingerprint, model, prompt_version)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id, level) DO UPDATE SET
            fingerprint = excluded.fingerprint,
            model = excluded.model,
            prompt_version = excluded.prompt_version,
            created_at = CURRENT_TIMESTAMP
        """,
        (source_id, EVENT_LEVEL, fingerprint, model, prompt_version),
    )


def _parse_events(raw: str) -> list[dict] | None:
    """Tolerantly parse a JSON array of events. Returns None on unparseable output."""
    start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end < start:
        return None
    try:
        parsed = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, list):
        return None

    events: list[dict] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        summary = str(item.get("summary", "")).strip() or None
        events.append({"title": title, "summary": summary})
        if len(events) >= MAX_EVENTS_PER_SOURCE:
            break
    return events


def _fingerprint(model: str, prompt_version: str, source_text: str) -> str:
    payload = f"{model}\x1f{prompt_version}\x1f{source_text}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
