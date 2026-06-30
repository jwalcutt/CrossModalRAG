from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crossmodalrag.chunking import chunk_markdown
from crossmodalrag.embed.provider import EmbeddingProvider
from crossmodalrag.ingest._embed import embed_source_chunks, purge_source_embeddings


def ingest_notes(
    conn: sqlite3.Connection,
    vault_path: Path,
    embedder: EmbeddingProvider | None = None,
    progress=None,
) -> int:
    if not vault_path.exists():
        raise FileNotFoundError(f"Vault path does not exist: {vault_path}")
    inserted_chunks = 0
    md_files = sorted(vault_path.rglob("*.md"))
    total = len(md_files)
    for scanned, path in enumerate(md_files, start=1):
        if progress is not None:
            progress(scanned, total)
        text = path.read_text(encoding="utf-8", errors="ignore")
        stat = path.stat()
        source_uri = str(path.resolve())
        # Prefer an explicit, content-declared date (deterministic across machines/checkouts and
        # useful for time-aware layers like drift); fall back to file mtime when absent.
        timestamp = _parse_note_date(text) or _iso_mtime(stat.st_mtime)
        source_fingerprint = _source_fingerprint(text)
        metadata_json = json.dumps({"bytes": stat.st_size, "fingerprint": source_fingerprint})
        source_id, unchanged = _upsert_note_source(
            conn=conn,
            source_uri=source_uri,
            source_fingerprint=source_fingerprint,
            timestamp=timestamp,
            title=path.stem,
            metadata_json=metadata_json,
        )
        if unchanged:
            continue

        purge_source_embeddings(conn, source_id)
        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))
        new_chunks: list[tuple[int, str]] = []
        for idx, chunk in enumerate(chunk_markdown(text)):
            cur = conn.execute(
                """
                INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
                VALUES (?, ?, ?, ?)
                """,
                (source_id, idx, chunk, json.dumps({"modality": "text", "source_type": "note"})),
            )
            new_chunks.append((int(cur.lastrowid), chunk))
            inserted_chunks += 1
        embed_source_chunks(conn, embedder, new_chunks)
    conn.commit()
    return inserted_chunks


def _iso_mtime(mtime: float) -> str:
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


# A YAML-frontmatter `date:`/`created:` value (Obsidian-style) or a leading `Date:` line, taken as
# the note's authoritative timestamp. Date-only values are normalized to midnight UTC. Kept tolerant:
# any unrecognized/malformed value yields None so ingestion falls back to mtime.
_FRONTMATTER_RE = re.compile(r"\A﻿?---\s*\n(.*?)\n---\s*(?:\n|\Z)", re.DOTALL)
_FRONTMATTER_DATE_RE = re.compile(r"^(?:date|created)\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE)
_BODY_DATE_RE = re.compile(r"^Date\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE)
_HEAD_LINES = 12  # only scan near the top of the body for a `Date:` line


def _parse_note_date(text: str) -> str | None:
    """Extract an explicit note date (frontmatter `date:`/`created:` or a leading `Date:` line).

    Returns a normalized ISO-8601 UTC timestamp, or None when no parseable date is present.
    """
    raw: str | None = None
    frontmatter = _FRONTMATTER_RE.match(text)
    if frontmatter:
        match = _FRONTMATTER_DATE_RE.search(frontmatter.group(1))
        if match:
            raw = match.group(1)
    if raw is None:
        # Scan only the first few non-frontmatter lines for a `Date:` line.
        body = text[frontmatter.end():] if frontmatter else text
        head = "\n".join(body.splitlines()[:_HEAD_LINES])
        match = _BODY_DATE_RE.search(head)
        if match:
            raw = match.group(1)
    if raw is None:
        return None
    return _normalize_date(raw)


def _normalize_date(raw: str) -> str | None:
    value = raw.strip().strip('"').strip("'").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        # Date-only fallback (YYYY-MM-DD) when the full ISO parse fails on a bare date.
        try:
            dt = datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _source_fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _upsert_note_source(
    conn: sqlite3.Connection,
    source_uri: str,
    source_fingerprint: str,
    timestamp: str,
    title: str,
    metadata_json: str,
) -> tuple[int, bool]:
    rows = conn.execute(
        """
        SELECT id, source_fingerprint, timestamp FROM sources
        WHERE source_type = ? AND source_uri = ?
        ORDER BY id ASC
        """,
        ("note", source_uri),
    ).fetchall()

    if not rows:
        cur = conn.execute(
            """
            INSERT INTO sources (source_type, source_uri, source_fingerprint, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("note", source_uri, source_fingerprint, timestamp, title, metadata_json),
        )
        return int(cur.lastrowid), False

    canonical_id = int(rows[-1]["id"])
    existing_fingerprint = rows[-1]["source_fingerprint"]
    existing_timestamp = rows[-1]["timestamp"]
    is_legacy_unchanged = existing_fingerprint is None and existing_timestamp == timestamp
    is_unchanged = existing_fingerprint == source_fingerprint or is_legacy_unchanged

    # Collapse historical duplicates so each note path has a single source row.
    for row in rows[:-1]:
        old_id = int(row["id"])
        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (old_id,))
        conn.execute("DELETE FROM sources WHERE id = ?", (old_id,))

    if is_unchanged:
        # Backfill fingerprint for legacy rows while preserving recency semantics for unchanged content.
        if existing_fingerprint is None:
            conn.execute(
                """
                UPDATE sources
                SET source_fingerprint = ?, title = ?, metadata_json = ?
                WHERE id = ?
                """,
                (source_fingerprint, title, metadata_json, canonical_id),
            )
        return canonical_id, True

    conn.execute(
        """
        UPDATE sources
        SET source_fingerprint = ?, timestamp = ?, title = ?, metadata_json = ?
        WHERE id = ?
        """,
        (source_fingerprint, timestamp, title, metadata_json, canonical_id),
    )
    return canonical_id, False
