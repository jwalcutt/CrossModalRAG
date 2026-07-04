from __future__ import annotations

import sqlite3
from dataclasses import dataclass

# Edges whose endpoints sit at level 0 reference evidence_chunks.id; levels
# 1-3 reference memory_nodes.id. Drill-down only follows these downward relations.
DOWNWARD_RELATIONS = ("contains", "derived_from")
EVIDENCE_LEVEL = 0


@dataclass(frozen=True)
class MemoryNode:
    id: int
    level: int
    node_type: str
    title: str | None
    content: str | None
    time_start: str | None
    time_end: str | None
    derivation_fingerprint: str | None
    model: str | None
    prompt_version: str | None
    confidence: float | None
    metadata_json: str | None
    created_at: str | None


def insert_node(
    conn: sqlite3.Connection,
    *,
    level: int,
    node_type: str,
    title: str | None = None,
    content: str | None = None,
    time_start: str | None = None,
    time_end: str | None = None,
    derivation_fingerprint: str | None = None,
    model: str | None = None,
    prompt_version: str | None = None,
    confidence: float | None = None,
    metadata: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO memory_nodes (
            level, node_type, title, content, time_start, time_end,
            derivation_fingerprint, model, prompt_version, confidence, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            level,
            node_type,
            title,
            content,
            time_start,
            time_end,
            derivation_fingerprint,
            model,
            prompt_version,
            confidence,
            metadata,
        ),
    )
    return int(cur.lastrowid)


def get_node(conn: sqlite3.Connection, node_id: int) -> MemoryNode | None:
    row = conn.execute("SELECT * FROM memory_nodes WHERE id = ?", (node_id,)).fetchone()
    return _row_to_node(row) if row is not None else None


def list_nodes(
    conn: sqlite3.Connection,
    *,
    level: int | None = None,
    node_type: str | None = None,
) -> list[MemoryNode]:
    sql = "SELECT * FROM memory_nodes"
    clauses: list[str] = []
    params: list[object] = []
    if level is not None:
        clauses.append("level = ?")
        params.append(level)
    if node_type is not None:
        clauses.append("node_type = ?")
        params.append(node_type)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id ASC"
    return [_row_to_node(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def add_edge(
    conn: sqlite3.Connection,
    parent_level: int,
    parent_id: int,
    child_level: int,
    child_id: int,
    relation: str,
    weight: float = 1.0,
) -> None:
    conn.execute(
        """
        INSERT INTO memory_edges (parent_level, parent_id, child_level, child_id, relation, weight)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(parent_level, parent_id, child_level, child_id, relation) DO NOTHING
        """,
        (parent_level, parent_id, child_level, child_id, relation, weight),
    )


def get_children(
    conn: sqlite3.Connection,
    parent_level: int,
    parent_id: int,
    relation: str | None = None,
) -> list[tuple[int, int]]:
    sql = "SELECT child_level, child_id FROM memory_edges WHERE parent_level = ? AND parent_id = ?"
    params: list[object] = [parent_level, parent_id]
    if relation is not None:
        sql += " AND relation = ?"
        params.append(relation)
    sql += " ORDER BY child_level, child_id"
    return [(int(r["child_level"]), int(r["child_id"])) for r in conn.execute(sql, tuple(params)).fetchall()]


def get_parents(
    conn: sqlite3.Connection,
    child_level: int,
    child_id: int,
) -> list[tuple[int, int]]:
    rows = conn.execute(
        """
        SELECT parent_level, parent_id FROM memory_edges
        WHERE child_level = ? AND child_id = ?
        ORDER BY parent_level, parent_id
        """,
        (child_level, child_id),
    ).fetchall()
    return [(int(r["parent_level"]), int(r["parent_id"])) for r in rows]


def delete_node(conn: sqlite3.Connection, node_id: int) -> None:
    """Remove a node and any edges incident on it (at levels 1-3).

    Needed for re-derivation in later phases; kept here so the substrate owns
    its own cleanup and never leaves dangling edges behind.
    """
    conn.execute(
        """
        DELETE FROM memory_edges
        WHERE (parent_level != ? AND parent_id = ?)
           OR (child_level != ? AND child_id = ?)
        """,
        (EVIDENCE_LEVEL, node_id, EVIDENCE_LEVEL, node_id),
    )
    conn.execute("DELETE FROM node_embeddings WHERE node_id = ?", (node_id,))
    conn.execute("DELETE FROM recall_cards WHERE node_id = ?", (node_id,))
    # derived caches keyed by this node (additive/separable; cleaned with the node so no
    # orphan rows survive a re-derivation that drops the node).
    conn.execute("DELETE FROM distilled_nodes WHERE node_id = ?", (node_id,))
    conn.execute("DELETE FROM drift_snapshots WHERE concept_id = ?", (node_id,))
    conn.execute("DELETE FROM memory_nodes WHERE id = ?", (node_id,))


def resolve_to_evidence(
    conn: sqlite3.Connection,
    level: int,
    node_id: int,
    max_depth: int = 8,
) -> list[int]:
    """Walk downward edges from a node to the distinct L0 evidence_chunks ids it rests on.

    Follows only DOWNWARD_RELATIONS, guards against cycles via a visited set, and
    bounds traversal with max_depth. An L0 endpoint (level 0) is collected as evidence.
    """
    evidence: list[int] = []
    seen_evidence: set[int] = set()
    visited: set[tuple[int, int]] = set()
    # Stack of (level, id, depth).
    stack: list[tuple[int, int, int]] = [(level, node_id, 0)]

    while stack:
        cur_level, cur_id, depth = stack.pop()
        if cur_level == EVIDENCE_LEVEL:
            if cur_id not in seen_evidence:
                seen_evidence.add(cur_id)
                evidence.append(cur_id)
            continue
        if (cur_level, cur_id) in visited or depth >= max_depth:
            continue
        visited.add((cur_level, cur_id))
        for child_level, child_id in _downward_children(conn, cur_level, cur_id):
            stack.append((child_level, child_id, depth + 1))

    # An edge may reference a chunk id re-issued by a re-chunk; a dead id is not
    # evidence. Filtering here keeps every caller honest — integrity checks,
    # drill-down retrieval, and distilled core-evidence subsets alike.
    return sorted(_existing_chunk_ids(conn, evidence))


def _existing_chunk_ids(conn: sqlite3.Connection, chunk_ids: list[int]) -> list[int]:
    existing: list[int] = []
    batch_size = 500  # stay under SQLite's bound-parameter limit
    for start in range(0, len(chunk_ids), batch_size):
        batch = chunk_ids[start : start + batch_size]
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(
            f"SELECT id FROM evidence_chunks WHERE id IN ({placeholders})", batch
        ).fetchall()
        existing.extend(int(r["id"]) for r in rows)
    return existing


def _downward_children(conn: sqlite3.Connection, parent_level: int, parent_id: int) -> list[tuple[int, int]]:
    placeholders = ",".join("?" for _ in DOWNWARD_RELATIONS)
    rows = conn.execute(
        f"""
        SELECT child_level, child_id FROM memory_edges
        WHERE parent_level = ? AND parent_id = ? AND relation IN ({placeholders})
        """,
        (parent_level, parent_id, *DOWNWARD_RELATIONS),
    ).fetchall()
    return [(int(r["child_level"]), int(r["child_id"])) for r in rows]


def _row_to_node(row: sqlite3.Row) -> MemoryNode:
    return MemoryNode(
        id=int(row["id"]),
        level=int(row["level"]),
        node_type=str(row["node_type"]),
        title=row["title"],
        content=row["content"],
        time_start=row["time_start"],
        time_end=row["time_end"],
        derivation_fingerprint=row["derivation_fingerprint"],
        model=row["model"],
        prompt_version=row["prompt_version"],
        confidence=row["confidence"],
        metadata_json=row["metadata_json"],
        created_at=row["created_at"],
    )
