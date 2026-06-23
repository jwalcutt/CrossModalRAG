from __future__ import annotations

import json

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.episodes import EVENT_LEVEL, _project_key, build_episodes
from crossmodalrag.memory.integrity import find_dangling_edges, find_unsupported_nodes
from crossmodalrag.memory.store import add_edge, delete_node, get_children, list_nodes, resolve_to_evidence

GAP = 3600  # 1 hour, for tests


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def _add_event(conn, *, source_uri, source_type, time_start, title) -> tuple[int, int]:
    """Create an L0 chunk and an L1 event derived from it. Returns (event_id, chunk_id)."""
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp) VALUES (?, ?, ?)",
        (source_type, source_uri, time_start),
    )
    sid = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (sid, 0, f"chunk for {title}"),
    )
    chunk_id = int(cur.lastrowid)
    cur = conn.execute(
        """
        INSERT INTO memory_nodes (level, node_type, title, time_start, time_end, metadata_json)
        VALUES (1, 'event', ?, ?, ?, ?)
        """,
        (title, time_start, time_start, json.dumps({"source_id": sid, "source_uri": source_uri, "source_type": source_type})),
    )
    event_id = int(cur.lastrowid)
    add_edge(conn, EVENT_LEVEL, event_id, 0, chunk_id, "derived_from")
    conn.commit()
    return event_id, chunk_id


def _snapshot(conn):
    nodes = conn.execute(
        "SELECT id, level, node_type, title, derivation_fingerprint FROM memory_nodes ORDER BY id"
    ).fetchall()
    edges = conn.execute(
        "SELECT parent_level, parent_id, child_level, child_id, relation FROM memory_edges ORDER BY id"
    ).fetchall()
    return [tuple(n) for n in nodes], [tuple(e) for e in edges]


def test_project_key_derivation() -> None:
    assert _project_key("git_commit", "/home/me/repo@abc123") == "/home/me/repo"
    assert _project_key("note", "/vault/projects/a.md") == "/vault/projects"
    assert _project_key("note", "") == "note"
    # Cross-modal: PDFs/images group by their containing directory, like notes — so a
    # note + pdf + image in the same folder share a project (one cross-modal episode).
    assert _project_key("pdf", "/vault/documents/spec.pdf") == "/vault/documents"
    assert _project_key("image", "/vault/documents/diagram.png") == "/vault/documents"
    assert (
        _project_key("note", "/vault/documents/notes.md")
        == _project_key("pdf", "/vault/documents/spec.pdf")
    )


def test_grouping_by_project_and_time_gap(conn) -> None:
    e1, c1 = _add_event(conn, source_uri="/repo@s1", source_type="git_commit", time_start="2026-01-01T00:00:00+00:00", title="commit 1")
    e2, c2 = _add_event(conn, source_uri="/repo@s2", source_type="git_commit", time_start="2026-01-01T00:30:00+00:00", title="commit 2")
    e3, c3 = _add_event(conn, source_uri="/repo@s3", source_type="git_commit", time_start="2026-01-02T00:00:00+00:00", title="commit 3")
    e4, c4 = _add_event(conn, source_uri="/vault/a.md", source_type="note", time_start="2026-01-01T00:00:00+00:00", title="note 1")

    result = build_episodes(conn, gap_seconds=GAP)
    assert result.episodes_created == 3
    assert result.events_grouped == 4

    episodes = list_nodes(conn, level=2, node_type="episode")
    assert len(episodes) == 3

    # The episode that contains e1 should also contain e2 (within gap), and resolve to both chunks.
    by_members = {}
    for ep in episodes:
        members = frozenset(cid for (lvl, cid) in get_children(conn, 2, ep.id, relation="contains"))
        by_members[members] = ep
    assert frozenset({e1, e2}) in by_members
    assert frozenset({e3}) in by_members
    assert frozenset({e4}) in by_members

    paired = by_members[frozenset({e1, e2})]
    assert resolve_to_evidence(conn, 2, paired.id) == sorted([c1, c2])


def test_build_episodes_is_idempotent(conn) -> None:
    _add_event(conn, source_uri="/repo@s1", source_type="git_commit", time_start="2026-01-01T00:00:00+00:00", title="c1")
    _add_event(conn, source_uri="/repo@s2", source_type="git_commit", time_start="2026-01-01T00:10:00+00:00", title="c2")

    first = build_episodes(conn, gap_seconds=GAP)
    assert first.episodes_created == 1
    snap1 = _snapshot(conn)

    second = build_episodes(conn, gap_seconds=GAP)
    assert second.episodes_created == 0
    assert second.episodes_kept == 1
    assert second.episodes_deleted == 0
    assert _snapshot(conn) == snap1  # identical, including ids


def test_l1_change_rebuilds_only_affected_episode(conn) -> None:
    e1, _ = _add_event(conn, source_uri="/repo@s1", source_type="git_commit", time_start="2026-01-01T00:00:00+00:00", title="c1")
    e2, _ = _add_event(conn, source_uri="/repo@s2", source_type="git_commit", time_start="2026-01-01T00:10:00+00:00", title="c2")
    e3, _ = _add_event(conn, source_uri="/repo@s3", source_type="git_commit", time_start="2026-01-03T00:00:00+00:00", title="c3")
    build_episodes(conn, gap_seconds=GAP)  # -> [e1,e2] and [e3]

    # Remove e3; its singleton episode becomes stale.
    delete_node(conn, e3)
    result = build_episodes(conn, gap_seconds=GAP)

    assert result.episodes_kept == 1     # [e1,e2] unchanged
    assert result.episodes_deleted == 1  # [e3] gone
    assert result.episodes_created == 0
    assert len(list_nodes(conn, level=2, node_type="episode")) == 1
    assert find_dangling_edges(conn) == []


def test_integrity_clean_after_build(conn) -> None:
    _add_event(conn, source_uri="/vault/a.md", source_type="note", time_start="2026-01-01T00:00:00+00:00", title="n1")
    _add_event(conn, source_uri="/vault/b.md", source_type="note", time_start="2026-01-01T00:05:00+00:00", title="n2")
    build_episodes(conn, gap_seconds=GAP)
    assert find_unsupported_nodes(conn) == []
    assert find_dangling_edges(conn) == []


def test_no_events_yields_no_episodes(conn) -> None:
    result = build_episodes(conn, gap_seconds=GAP)
    assert result.episodes_created == 0
    assert list_nodes(conn, level=2) == []
