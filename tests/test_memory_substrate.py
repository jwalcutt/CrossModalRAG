from __future__ import annotations

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory import (
    add_edge,
    delete_node,
    find_dangling_edges,
    find_unsupported_nodes,
    get_children,
    get_node,
    get_parents,
    insert_node,
    list_nodes,
    resolve_to_evidence,
)


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def _add_chunk(conn, text: str = "evidence body") -> int:
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri) VALUES (?, ?)",
        ("note", f"/abs/{text}.md"),
    )
    src = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (src, 0, text),
    )
    return int(cur.lastrowid)


def test_init_db_creates_memory_tables(conn) -> None:
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert {"memory_nodes", "memory_edges"} <= tables


def test_insert_and_get_node_round_trip(conn) -> None:
    node_id = insert_node(
        conn,
        level=1,
        node_type="event",
        title="Fixed parser",
        content="Resolved the bounds check bug.",
        confidence=0.8,
    )
    node = get_node(conn, node_id)
    assert node is not None
    assert node.level == 1
    assert node.node_type == "event"
    assert node.title == "Fixed parser"
    assert node.confidence == 0.8
    assert get_node(conn, 9999) is None


def test_list_nodes_filters(conn) -> None:
    insert_node(conn, level=1, node_type="event", title="e1")
    insert_node(conn, level=2, node_type="episode", title="ep1")
    insert_node(conn, level=3, node_type="concept", title="c1")

    assert [n.title for n in list_nodes(conn, level=1)] == ["e1"]
    assert [n.title for n in list_nodes(conn, node_type="concept")] == ["c1"]
    assert len(list_nodes(conn)) == 3


def test_add_edge_is_idempotent(conn) -> None:
    event = insert_node(conn, level=1, node_type="event")
    chunk = _add_chunk(conn)
    add_edge(conn, 1, event, 0, chunk, "derived_from")
    add_edge(conn, 1, event, 0, chunk, "derived_from")  # duplicate ignored
    assert get_children(conn, 1, event) == [(0, chunk)]
    assert get_parents(conn, 0, chunk) == [(1, event)]


def test_resolve_to_evidence_traverses_hierarchy(conn) -> None:
    # L0 chunks
    c1 = _add_chunk(conn, "alpha")
    c2 = _add_chunk(conn, "beta")
    # L1 events -> chunks
    e1 = insert_node(conn, level=1, node_type="event", title="e1")
    e2 = insert_node(conn, level=1, node_type="event", title="e2")
    add_edge(conn, 1, e1, 0, c1, "derived_from")
    add_edge(conn, 1, e2, 0, c2, "derived_from")
    # L2 episode -> events
    ep = insert_node(conn, level=2, node_type="episode", title="ep")
    add_edge(conn, 2, ep, 1, e1, "contains")
    add_edge(conn, 2, ep, 1, e2, "contains")
    # L3 concept -> episode
    concept = insert_node(conn, level=3, node_type="concept", title="c")
    add_edge(conn, 3, concept, 2, ep, "contains")

    assert resolve_to_evidence(conn, 3, concept) == sorted([c1, c2])
    assert resolve_to_evidence(conn, 1, e1) == [c1]


def test_relates_to_edges_not_followed_in_drilldown(conn) -> None:
    c1 = _add_chunk(conn, "alpha")
    concept_a = insert_node(conn, level=3, node_type="concept", title="A")
    concept_b = insert_node(conn, level=3, node_type="concept", title="B")
    add_edge(conn, 3, concept_a, 0, c1, "contains")
    # Lateral link must NOT pull B's (nonexistent) evidence into A.
    add_edge(conn, 3, concept_a, 3, concept_b, "relates_to")

    assert resolve_to_evidence(conn, 3, concept_a) == [c1]
    assert resolve_to_evidence(conn, 3, concept_b) == []


def test_find_unsupported_nodes_flags_only_ungrounded(conn) -> None:
    c1 = _add_chunk(conn, "alpha")
    grounded = insert_node(conn, level=1, node_type="event")
    add_edge(conn, 1, grounded, 0, c1, "derived_from")
    floating = insert_node(conn, level=1, node_type="event")  # no evidence

    assert find_unsupported_nodes(conn) == [floating]


def test_find_dangling_edges(conn) -> None:
    event = insert_node(conn, level=1, node_type="event")
    # Edge to a nonexistent L0 chunk id.
    add_edge(conn, 1, event, 0, 4242, "derived_from")
    dangling = find_dangling_edges(conn)
    assert len(dangling) == 1


def test_resolve_handles_cycles(conn) -> None:
    a = insert_node(conn, level=2, node_type="episode")
    b = insert_node(conn, level=2, node_type="episode")
    # Pathological cycle between same-level nodes; must terminate.
    add_edge(conn, 2, a, 2, b, "contains")
    add_edge(conn, 2, b, 2, a, "contains")
    assert resolve_to_evidence(conn, 2, a) == []


def test_delete_node_removes_incident_edges(conn) -> None:
    c1 = _add_chunk(conn, "alpha")
    event = insert_node(conn, level=1, node_type="event")
    episode = insert_node(conn, level=2, node_type="episode")
    add_edge(conn, 1, event, 0, c1, "derived_from")
    add_edge(conn, 2, episode, 1, event, "contains")

    delete_node(conn, event)
    assert get_node(conn, event) is None
    # The episode->event edge and event->chunk edge are gone; no dangling left.
    assert find_dangling_edges(conn) == []
    assert get_children(conn, 2, episode) == []
