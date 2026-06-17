from __future__ import annotations

import sqlite3

from crossmodalrag.memory.store import EVIDENCE_LEVEL, resolve_to_evidence


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
