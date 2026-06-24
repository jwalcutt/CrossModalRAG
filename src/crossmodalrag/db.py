from __future__ import annotations

import sqlite3
from pathlib import Path


DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    source_fingerprint TEXT,
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

CREATE TABLE IF NOT EXISTS chunk_embeddings (
    chunk_id INTEGER PRIMARY KEY,
    model TEXT NOT NULL,
    dim INTEGER NOT NULL,
    vector BLOB NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(chunk_id) REFERENCES evidence_chunks(id)
);

CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_model ON chunk_embeddings(model);

CREATE TABLE IF NOT EXISTS memory_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level INTEGER NOT NULL CHECK(level IN (1,2,3)),
    node_type TEXT NOT NULL,
    title TEXT,
    content TEXT,
    time_start TEXT,
    time_end TEXT,
    derivation_fingerprint TEXT,
    model TEXT,
    prompt_version TEXT,
    confidence REAL,
    metadata_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_memory_nodes_level ON memory_nodes(level);
CREATE INDEX IF NOT EXISTS idx_memory_nodes_type ON memory_nodes(node_type);

CREATE TABLE IF NOT EXISTS node_embeddings (
    node_id INTEGER PRIMARY KEY,
    model TEXT NOT NULL,
    dim INTEGER NOT NULL,
    vector BLOB NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(node_id) REFERENCES memory_nodes(id)
);

CREATE INDEX IF NOT EXISTS idx_node_embeddings_model ON node_embeddings(model);

CREATE TABLE IF NOT EXISTS memory_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_level INTEGER NOT NULL,
    parent_id INTEGER NOT NULL,
    child_level INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    relation TEXT NOT NULL,
    weight REAL DEFAULT 1.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(parent_level, parent_id, child_level, child_id, relation)
);

CREATE INDEX IF NOT EXISTS idx_memory_edges_parent ON memory_edges(parent_level, parent_id);
CREATE INDEX IF NOT EXISTS idx_memory_edges_child ON memory_edges(child_level, child_id);

CREATE TABLE IF NOT EXISTS memory_derivations (
    source_id INTEGER NOT NULL,
    level INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    model TEXT,
    prompt_version TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, level)
);

CREATE TABLE IF NOT EXISTS queries_eval (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_text TEXT NOT NULL,
    expected_source_uris TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- append-only usage signal (interaction history). Additive and SEPARABLE — it is
-- never part of any content/derivation fingerprint, so clearing it restores the exact
-- baseline. Endpoints are polymorphic (target_kind 'chunk' -> evidence_chunks.id,
-- 'node' -> memory_nodes.id) so there is NO DB-level FK (SQLite can't FK to two tables);
-- integrity is enforced in app code + tests, mirroring memory_edges. ``event_at`` is the
-- (injectable) time the event happened, used for time-decay; ``created_at`` is audit only.
CREATE TABLE IF NOT EXISTS usage_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_kind TEXT NOT NULL,
    target_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0,
    event_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_usage_events_target ON usage_events(target_kind, target_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_event_at ON usage_events(event_at);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    _run_migrations(conn)
    conn.commit()


def _run_migrations(conn: sqlite3.Connection) -> None:
    _ensure_column(conn, table_name="sources", column_name="source_fingerprint", column_def="TEXT")
    _ensure_column(conn, table_name="memory_nodes", column_name="centrality", column_def="REAL")


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_def: str,
) -> None:
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {str(row["name"]) for row in columns}
    if column_name in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
