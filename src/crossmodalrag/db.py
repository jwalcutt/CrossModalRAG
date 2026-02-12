from __future__ import annotations

import sqlite3
from pathlib import Path


DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    timestamp TEXT,
    title TEXT,
    metadata_json TEXT,
    UNIQUE(source_type, source_uri, timestamp)
);

CREATE TABLE IF NOT EXISTS evidence_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    metadata_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(source_id) REFERENCES sources(id)
);

CREATE INDEX IF NOT EXISTS idx_evidence_source_id ON evidence_chunks(source_id);

CREATE TABLE IF NOT EXISTS queries_eval (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_text TEXT NOT NULL,
    expected_source_uris TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()

