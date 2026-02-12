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
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO sources (source_type, source_uri, timestamp, title, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "note",
                source_uri,
                _iso_mtime(stat.st_mtime),
                path.stem,
                json.dumps({"bytes": stat.st_size}),
            ),
        )
        source_id = cur.lastrowid
        if not source_id:
            row = conn.execute(
                """
                SELECT id FROM sources
                WHERE source_type = ? AND source_uri = ? AND timestamp = ?
                """,
                ("note", source_uri, _iso_mtime(stat.st_mtime)),
            ).fetchone()
            if not row:
                continue
            source_id = int(row["id"])
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

