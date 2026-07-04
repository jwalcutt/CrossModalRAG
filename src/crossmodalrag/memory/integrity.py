from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from crossmodalrag.memory.store import EVIDENCE_LEVEL, resolve_to_evidence


def memory_stats(conn: sqlite3.Connection) -> dict:
    """Read-only hierarchical-memory stats + integrity. Stable JSON contract for `mem memory-stats`."""
    from crossmodalrag.embed.store import count_node_embeddings

    by_level = count_nodes_by_level(conn)
    unsupported = find_unsupported_nodes(conn)
    dangling = find_dangling_edges(conn)
    relates_edges = conn.execute(
        "SELECT COUNT(*) AS n FROM memory_edges WHERE relation = 'relates_to'"
    ).fetchone()["n"]
    distilled_count = conn.execute("SELECT COUNT(*) AS n FROM distilled_nodes").fetchone()["n"]
    drift_count = conn.execute("SELECT COUNT(*) AS n FROM drift_snapshots").fetchone()["n"]
    top_central = conn.execute(
        "SELECT id, level, title, centrality FROM memory_nodes "
        "WHERE centrality IS NOT NULL ORDER BY centrality DESC, id ASC LIMIT 3"
    ).fetchall()
    return {
        "total_nodes": sum(by_level.values()),
        "nodes_by_level": {str(level): by_level.get(level, 0) for level in (1, 2, 3)},
        "nodes_by_type": dict(count_nodes_by_type(conn)),
        "edges": count_edges(conn),
        "relates_edges": int(relates_edges),
        "node_embeddings": count_node_embeddings(conn),
        "distilled_nodes": int(distilled_count),
        "drift_snapshots": int(drift_count),
        "top_central": [
            {
                "node_id": int(r["id"]),
                "level": int(r["level"]),
                "title": r["title"],
                "centrality": float(r["centrality"]),
            }
            for r in top_central
        ],
        "integrity": {
            "unsupported_count": len(unsupported),
            "unsupported_ids": unsupported,
            "dangling_count": len(dangling),
            "dangling_ids": dangling,
        },
    }


def find_unsupported_nodes(conn: sqlite3.Connection) -> list[int]:
    """Return ids of L1-L3 nodes that do not trace down to any L0 evidence chunk.

    This is the structural provenance invariant: every higher-level memory node
    must rest on at least one piece of L0 evidence. A healthy store returns [].
    """
    unsupported: list[int] = []
    for row in conn.execute("SELECT id, level FROM memory_nodes ORDER BY id ASC").fetchall():
        node_id = int(row["id"])
        if not resolve_to_evidence(conn, int(row["level"]), node_id):
            unsupported.append(node_id)
    return unsupported


def find_dangling_edges(conn: sqlite3.Connection) -> list[int]:
    """Return ids of edges whose parent or child endpoint no longer exists.

    Endpoints at level 0 must exist in evidence_chunks; levels 1-3 in memory_nodes.
    """
    node_ids = {int(r["id"]) for r in conn.execute("SELECT id FROM memory_nodes").fetchall()}
    chunk_ids = {int(r["id"]) for r in conn.execute("SELECT id FROM evidence_chunks").fetchall()}

    dangling: list[int] = []
    rows = conn.execute(
        "SELECT id, parent_level, parent_id, child_level, child_id FROM memory_edges ORDER BY id ASC"
    ).fetchall()
    for row in rows:
        endpoints = (
            (int(row["parent_level"]), int(row["parent_id"])),
            (int(row["child_level"]), int(row["child_id"])),
        )
        for endpoint_level, endpoint_id in endpoints:
            valid = chunk_ids if endpoint_level == EVIDENCE_LEVEL else node_ids
            if endpoint_id not in valid:
                dangling.append(int(row["id"]))
                break
    return dangling


@dataclass(frozen=True)
class EvidenceRepairResult:
    events_checked: int
    events_repaired: int
    edges_removed: int
    edges_added: int
    orphaned_event_ids: list[int]


def repair_evidence_edges(conn: sqlite3.Connection) -> EvidenceRepairResult:
    """Re-anchor L1 events whose ``derived_from`` edges point at dead chunk ids.

    Re-chunking a source (content change, or a chunker-version bump picked up by
    ``mem sync``) deletes and re-inserts its evidence_chunks rows with new ids.
    Event extraction re-derives only when the source *text* changed, so a
    text-identical re-chunk leaves that source's events pointing at chunk ids
    that no longer exist. The extraction grain is source-level — an event links
    to every chunk of its source — so the repair is deterministic and needs no
    LLM: drop the dead edges and link the event to the source's current chunks.

    Events whose source (or its chunks) no longer exists cannot be re-anchored;
    they are reported in ``orphaned_event_ids`` and left untouched — nothing is
    deleted by this function.
    """
    chunk_ids = {int(r["id"]) for r in conn.execute("SELECT id FROM evidence_chunks").fetchall()}
    source_ids = {int(r["id"]) for r in conn.execute("SELECT id FROM sources").fetchall()}

    events_checked = events_repaired = edges_removed = edges_added = 0
    orphaned: list[int] = []

    events = conn.execute(
        "SELECT id, metadata_json FROM memory_nodes WHERE level = 1 ORDER BY id ASC"
    ).fetchall()
    for event in events:
        event_id = int(event["id"])
        events_checked += 1
        edges = conn.execute(
            """
            SELECT id, child_id FROM memory_edges
            WHERE parent_level = 1 AND parent_id = ? AND child_level = ?
              AND relation = 'derived_from'
            """,
            (event_id, EVIDENCE_LEVEL),
        ).fetchall()
        dead = [(int(e["id"]), int(e["child_id"])) for e in edges if int(e["child_id"]) not in chunk_ids]
        if not dead:
            continue

        source_id = _event_source_id(event["metadata_json"])
        current_chunks: list[int] = []
        if source_id is not None and source_id in source_ids:
            current_chunks = [
                int(r["id"])
                for r in conn.execute(
                    "SELECT id FROM evidence_chunks WHERE source_id = ? ORDER BY chunk_index ASC",
                    (source_id,),
                ).fetchall()
            ]
        if not current_chunks:
            orphaned.append(event_id)
            continue

        for edge_id, _ in dead:
            conn.execute("DELETE FROM memory_edges WHERE id = ?", (edge_id,))
        edges_removed += len(dead)
        live = {int(e["child_id"]) for e in edges} - {cid for _, cid in dead}
        for chunk_id in current_chunks:
            if chunk_id in live:
                continue
            conn.execute(
                """
                INSERT INTO memory_edges (parent_level, parent_id, child_level, child_id, relation, weight)
                VALUES (1, ?, ?, ?, 'derived_from', 1.0)
                ON CONFLICT(parent_level, parent_id, child_level, child_id, relation) DO NOTHING
                """,
                (event_id, EVIDENCE_LEVEL, chunk_id),
            )
            edges_added += 1
        events_repaired += 1

    conn.commit()
    return EvidenceRepairResult(
        events_checked=events_checked,
        events_repaired=events_repaired,
        edges_removed=edges_removed,
        edges_added=edges_added,
        orphaned_event_ids=orphaned,
    )


def _event_source_id(metadata_json: object) -> int | None:
    if not metadata_json:
        return None
    try:
        meta = json.loads(str(metadata_json))
    except json.JSONDecodeError:
        return None
    source_id = meta.get("source_id") if isinstance(meta, dict) else None
    return int(source_id) if isinstance(source_id, int) else None


def count_nodes_by_level(conn: sqlite3.Connection) -> dict[int, int]:
    rows = conn.execute(
        "SELECT level, COUNT(*) AS n FROM memory_nodes GROUP BY level ORDER BY level"
    ).fetchall()
    return {int(r["level"]): int(r["n"]) for r in rows}


def count_nodes_by_type(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT node_type, COUNT(*) AS n FROM memory_nodes GROUP BY node_type ORDER BY node_type"
    ).fetchall()
    return {str(r["node_type"]): int(r["n"]) for r in rows}


def count_edges(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) AS n FROM memory_edges").fetchone()["n"])
