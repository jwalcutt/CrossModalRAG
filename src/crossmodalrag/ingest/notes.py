from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from crossmodalrag.chunking import chunk_text


def ingest_notes(conn: sqlite3.Connection, vault_path: Path) -> int:
    if not vault_path.exists():
        raise FileNotFoundError(f"Vault path does not exist: {vault_path}")
    inserted_chunks = 0
    md_files = sorted(vault_path.rglob("*.md"))
    for path in md_files:
        text = path.read_text(encoding="utf-8", errors="ignore")
        stat = path.stat()
        source_uri = str(path.resolve())
        timestamp = _iso_mtime(stat.st_mtime)
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

        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))
        for idx, chunk in enumerate(chunk_text(text)):
            conn.execute(
                """
                INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)
                VALUES (?, ?, ?, ?)
                """,
                (source_id, idx, chunk, json.dumps({"modality": "text", "source_type": "note"})),
            )
            inserted_chunks += 1
    conn.commit()
    return inserted_chunks


def _iso_mtime(mtime: float) -> str:
    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


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
