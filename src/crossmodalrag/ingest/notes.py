from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
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
        metadata_json = json.dumps({"bytes": stat.st_size})
        source_id = _upsert_note_source(
            conn=conn,
            source_uri=source_uri,
            timestamp=timestamp,
            title=path.stem,
            metadata_json=metadata_json,
        )

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


def _upsert_note_source(
    conn: sqlite3.Connection,
    source_uri: str,
    timestamp: str,
    title: str,
    metadata_json: str,
) -> int:
    rows = conn.execute(
        """
        SELECT id FROM sources
        WHERE source_type = ? AND source_uri = ?
        ORDER BY id ASC
        """,
        ("note", source_uri),
    ).fetchall()

    if not rows:
        cur = conn.execute(
            """
            INSERT INTO sources (source_type, source_uri, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("note", source_uri, timestamp, title, metadata_json),
        )
        return int(cur.lastrowid)

    canonical_id = int(rows[-1]["id"])

    # Collapse historical duplicates so each note path has a single source row.
    for row in rows[:-1]:
        old_id = int(row["id"])
        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (old_id,))
        conn.execute("DELETE FROM sources WHERE id = ?", (old_id,))

    conn.execute(
        """
        UPDATE sources
        SET timestamp = ?, title = ?, metadata_json = ?
        WHERE id = ?
        """,
        (timestamp, title, metadata_json, canonical_id),
    )
    return canonical_id
