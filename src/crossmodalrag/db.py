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

-- cached active-recall cards, one per memory node. A derived cache (never part of any
-- content/derivation fingerprint); regenerated only when the node's content fingerprint changes
-- (or on --regenerate). Cleaned up with its node in memory.store.delete_node.
CREATE TABLE IF NOT EXISTS recall_cards (
    node_id INTEGER PRIMARY KEY,
    question TEXT NOT NULL,
    answer TEXT,
    fingerprint TEXT NOT NULL,
    model TEXT,
    prompt_version TEXT,
    generated_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- distilled (compact, retrieval-preserving) representation of an L2/L3 node, one row per node.
-- per node. ADDITIVE and OPT-IN: it never mutates memory_nodes/L0 and is excluded from every
-- content/derivation fingerprint, so dropping this table restores the Phase 1-4 baseline exactly.
-- ``node_id`` is polymorphic into memory_nodes (no DB FK, mirroring memory_edges); integrity is
-- enforced in app code + tests. ``core_evidence_json`` is a JSON list of REAL L0 evidence_chunks
-- ids (a subset of resolve_to_evidence) so provenance survives compression — never a paraphrase
-- that replaces the evidence. ``vector`` is the distilled summary embedding (float32 BLOB, model-
-- tagged), parallel to node_embeddings. Derivation lands in a later Phase 5 step; this is scaffold.
CREATE TABLE IF NOT EXISTS distilled_nodes (
    node_id INTEGER PRIMARY KEY,
    level INTEGER NOT NULL,
    summary TEXT,
    model TEXT,
    prompt_version TEXT,
    dim INTEGER,
    vector BLOB,
    core_evidence_json TEXT,
    derivation_fingerprint TEXT,
    confidence REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_distilled_nodes_fingerprint ON distilled_nodes(derivation_fingerprint);

-- per-concept, per-time-window drift snapshot (one row per concept x window). ADDITIVE
-- and SEPARABLE like distilled_nodes: excluded from all content/derivation fingerprints; dropping
-- it restores the baseline. ``concept_id`` is polymorphic into memory_nodes (no DB FK). The
-- ``prototype_vector`` is the centroid of the concept's member-event embeddings within
-- [window_start, window_end); ``drift_metric`` is the movement vs the previous window; ``support``
-- is the member count (explicit-uncertainty signal for thin windows). Computation lands in a later
-- Phase 5 step; this is scaffold.
CREATE TABLE IF NOT EXISTS drift_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    concept_id INTEGER NOT NULL,
    window_start TEXT,
    window_end TEXT,
    prototype_dim INTEGER,
    prototype_vector BLOB,
    drift_metric REAL,
    support INTEGER,
    derivation_fingerprint TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_drift_snapshots_concept ON drift_snapshots(concept_id);
CREATE INDEX IF NOT EXISTS idx_drift_snapshots_fingerprint ON drift_snapshots(derivation_fingerprint);

-- interactive chat history (conversations + their messages). ADDITIVE and SEPARABLE
-- (the usage_events precedent): never part of any content/derivation fingerprint and
-- never read by retrieval/derivation, so dropping both tables restores prior behavior
-- byte-identical. PRIVATE: stores raw query/answer text — local-only, opt-out
-- (CMRAG_SAVE_HISTORY / --no-save), clearable (`mem history --clear`). ``evidence_json``
-- is a point-in-time snapshot of the answer's full evidence ledger (same element shape
-- as the ask contract's ``evidence`` array, via generate.answer.evidence_payload);
-- chunk ids are re-issued on re-chunk, so the snapshot — never a live join — is the
-- source of truth for rendering history, and stored chunk_id/source_uri are best-effort
-- deep links only. ``started_at``/``updated_at`` are app-supplied UTC ISO (injectable,
-- the usage_events event_at precedent); ``created_at`` is audit only.
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    title TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations(updated_at);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL,
    turn_index INTEGER NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('user','assistant')),
    text TEXT NOT NULL,
    evidence_json TEXT,
    abstention_reason TEXT,
    truncated INTEGER NOT NULL DEFAULT 0,
    model TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(conversation_id) REFERENCES conversations(id),
    UNIQUE(conversation_id, turn_index, role)
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id, id);
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
