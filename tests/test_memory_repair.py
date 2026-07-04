"""Evidence-edge repair: events must survive a re-chunk that re-issues chunk ids.

Re-chunking a source deletes and re-inserts its evidence_chunks rows with new
ids. Extraction re-derives only when the source *text* changes, so a
text-identical re-chunk leaves that source's events pointing at dead chunk ids
(observed as 1720 dangling edges after a chunker-version sync). The repair
re-anchors those events deterministically to the source's current chunks.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.integrity import (
    find_dangling_edges,
    find_unsupported_nodes,
    repair_evidence_edges,
)
from crossmodalrag.memory.store import add_edge, insert_node, resolve_to_evidence


def _seed_source_with_event(conn: sqlite3.Connection, *, source_uri: str = "/vault/a.md") -> tuple[int, int, list[int]]:
    """Insert a source with two chunks and one event derived from both."""
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title, metadata_json)"
        " VALUES ('note', ?, '2026-01-01T00:00:00+00:00', 'a', '{}')",
        (source_uri,),
    )
    source_id = int(cur.lastrowid)
    chunk_ids = []
    for idx in range(2):
        cur = conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)"
            " VALUES (?, ?, ?, '{}')",
            (source_id, idx, f"chunk {idx}"),
        )
        chunk_ids.append(int(cur.lastrowid))
    event_id = insert_node(
        conn,
        level=1,
        node_type="event",
        title="an event",
        content="something happened",
        metadata=json.dumps({"source_id": source_id, "source_uri": source_uri}),
    )
    for chunk_id in chunk_ids:
        add_edge(conn, 1, event_id, 0, chunk_id, "derived_from")
    conn.commit()
    return source_id, event_id, chunk_ids


def _rechunk(conn: sqlite3.Connection, source_id: int, n_chunks: int = 3) -> list[int]:
    """Simulate a re-chunk: delete the source's chunks, re-insert with new ids."""
    conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))
    new_ids = []
    for idx in range(n_chunks):
        cur = conn.execute(
            "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text, metadata_json)"
            " VALUES (?, ?, ?, '{}')",
            (source_id, idx, f"new chunk {idx}"),
        )
        new_ids.append(int(cur.lastrowid))
    conn.commit()
    return new_ids


def test_repair_reanchors_event_to_current_chunks(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        source_id, event_id, _ = _seed_source_with_event(conn)
        new_chunk_ids = _rechunk(conn, source_id)
        assert find_dangling_edges(conn)  # broken before repair

        result = repair_evidence_edges(conn)

        assert result.events_repaired == 1
        assert result.edges_removed == 2
        assert result.edges_added == 3
        assert result.orphaned_event_ids == []
        assert find_dangling_edges(conn) == []
        assert resolve_to_evidence(conn, 1, event_id) == sorted(new_chunk_ids)
    finally:
        conn.close()


def test_repair_is_idempotent(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        source_id, _, _ = _seed_source_with_event(conn)
        _rechunk(conn, source_id)
        repair_evidence_edges(conn)

        second = repair_evidence_edges(conn)

        assert second.events_repaired == 0
        assert second.edges_removed == 0
        assert second.edges_added == 0
    finally:
        conn.close()


def test_repair_leaves_healthy_events_untouched(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        _, event_id, chunk_ids = _seed_source_with_event(conn)

        result = repair_evidence_edges(conn)

        assert result.events_checked == 1
        assert result.events_repaired == 0
        assert resolve_to_evidence(conn, 1, event_id) == sorted(chunk_ids)
    finally:
        conn.close()


def test_repair_reports_orphans_without_deleting(tmp_path: Path) -> None:
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        source_id, event_id, _ = _seed_source_with_event(conn)
        # The source vanishes entirely: nothing to re-anchor to.
        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))
        conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        conn.commit()

        result = repair_evidence_edges(conn)

        assert result.orphaned_event_ids == [event_id]
        assert result.events_repaired == 0
        # Nothing destroyed: the node and its (dangling) edges remain for inspection.
        assert conn.execute("SELECT COUNT(*) AS n FROM memory_nodes WHERE id = ?", (event_id,)).fetchone()["n"] == 1
        assert find_dangling_edges(conn)
    finally:
        conn.close()


def test_unsupported_check_sees_through_dead_chunk_ids(tmp_path: Path) -> None:
    # Regression: an event whose every derived_from edge points at a deleted chunk
    # used to count as "supported" because resolve_to_evidence returned dead ids.
    conn = connect(tmp_path / "mem.db")
    try:
        init_db(conn)
        source_id, event_id, _ = _seed_source_with_event(conn)
        conn.execute("DELETE FROM evidence_chunks WHERE source_id = ?", (source_id,))
        conn.commit()

        assert resolve_to_evidence(conn, 1, event_id) == []
        assert event_id in find_unsupported_nodes(conn)
    finally:
        conn.close()
