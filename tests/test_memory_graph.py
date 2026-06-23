from __future__ import annotations

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.graph import build_concept_cooccurrence, build_graph, compute_centrality
from crossmodalrag.memory.integrity import find_dangling_edges
from crossmodalrag.memory.store import add_edge, insert_node, resolve_to_evidence


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def _event(conn, title: str) -> int:
    cur = conn.execute("INSERT INTO sources (source_type, source_uri) VALUES ('note', ?)", (f"/v/{title}.md",))
    sid = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, 0, ?)", (sid, title)
    )
    chunk_id = int(cur.lastrowid)
    event_id = insert_node(conn, level=1, node_type="event", title=title)
    add_edge(conn, 1, event_id, 0, chunk_id, "derived_from")
    return event_id


def _contains(conn, parent_level, parent_id, child_id) -> None:
    add_edge(conn, parent_level, parent_id, 1, child_id, "contains")


def _relates_edges(conn):
    return conn.execute(
        "SELECT parent_id, child_id, weight FROM memory_edges WHERE relation = 'relates_to' "
        "ORDER BY parent_id, child_id"
    ).fetchall()


def test_cooccurrence_links_concepts_sharing_an_episode(conn) -> None:
    e1, e2, e3, e4, e5 = (_event(conn, f"e{i}") for i in range(1, 6))
    cA = insert_node(conn, level=3, node_type="concept", title="A")
    cB = insert_node(conn, level=3, node_type="concept", title="B")
    cC = insert_node(conn, level=3, node_type="concept", title="C")
    for ev in (e1, e4):
        _contains(conn, 3, cA, ev)
    for ev in (e2, e5):
        _contains(conn, 3, cB, ev)
    _contains(conn, 3, cC, e3)
    # Episodes: ep1{e1,e2}, ep2{e4,e5} both mix A & B -> shared twice; ep3{e3} only C.
    ep1 = insert_node(conn, level=2, node_type="episode", title="ep1")
    ep2 = insert_node(conn, level=2, node_type="episode", title="ep2")
    ep3 = insert_node(conn, level=2, node_type="episode", title="ep3")
    for ev in (e1, e2):
        _contains(conn, 2, ep1, ev)
    for ev in (e4, e5):
        _contains(conn, 2, ep2, ev)
    _contains(conn, 2, ep3, e3)
    conn.commit()

    created, deleted = build_concept_cooccurrence(conn)
    conn.commit()
    assert created == 1
    assert deleted == 0

    edges = _relates_edges(conn)
    assert len(edges) == 1
    assert (edges[0]["parent_id"], edges[0]["child_id"]) == (min(cA, cB), max(cA, cB))
    assert edges[0]["weight"] == 2.0  # shared ep1 and ep2


def test_cooccurrence_is_idempotent(conn) -> None:
    e1, e2 = _event(conn, "e1"), _event(conn, "e2")
    cA = insert_node(conn, level=3, node_type="concept", title="A")
    cB = insert_node(conn, level=3, node_type="concept", title="B")
    _contains(conn, 3, cA, e1)
    _contains(conn, 3, cB, e2)
    ep1 = insert_node(conn, level=2, node_type="episode", title="ep1")
    _contains(conn, 2, ep1, e1)
    _contains(conn, 2, ep1, e2)
    conn.commit()

    build_concept_cooccurrence(conn)
    conn.commit()
    first = [tuple(r) for r in _relates_edges(conn)]

    created, deleted = build_concept_cooccurrence(conn)
    conn.commit()
    assert created == 1 and deleted == 1  # replaced cleanly
    assert [tuple(r) for r in _relates_edges(conn)] == first


def test_cooccurrence_excluded_from_drilldown(conn) -> None:
    e1, e2 = _event(conn, "e1"), _event(conn, "e2")
    cA = insert_node(conn, level=3, node_type="concept", title="A")
    cB = insert_node(conn, level=3, node_type="concept", title="B")
    _contains(conn, 3, cA, e1)
    _contains(conn, 3, cB, e2)
    ep1 = insert_node(conn, level=2, node_type="episode", title="ep1")
    _contains(conn, 2, ep1, e1)
    _contains(conn, 2, ep1, e2)
    conn.commit()
    build_graph(conn)

    # cA must still resolve only to its own event's chunk, not cB's (relates_to not traversed).
    a_chunks = resolve_to_evidence(conn, 3, cA)
    b_chunks = resolve_to_evidence(conn, 3, cB)
    assert a_chunks and b_chunks and set(a_chunks).isdisjoint(b_chunks)
    assert find_dangling_edges(conn) == []


def test_centrality_hub_scores_highest_and_is_deterministic(conn) -> None:
    hub = insert_node(conn, level=3, node_type="concept", title="hub")
    events = [_event(conn, f"e{i}") for i in range(4)]
    episodes = []
    for ev in events:
        _contains(conn, 3, hub, ev)
        ep = insert_node(conn, level=2, node_type="episode", title=f"ep{ev}")
        _contains(conn, 2, ep, ev)
        episodes.append(ep)
    conn.commit()

    assert compute_centrality(conn) == len(events) + len(episodes) + 1
    conn.commit()
    scores = {int(r["id"]): r["centrality"] for r in conn.execute("SELECT id, centrality FROM memory_nodes")}

    assert abs(max(scores.values()) - 1.0) < 1e-9      # normalized
    assert max(scores, key=scores.get) == hub          # hub is most central
    # An event (in a concept + an episode) outranks its pendant episode.
    assert scores[events[0]] > scores[episodes[0]]

    # Deterministic across recomputation.
    compute_centrality(conn)
    conn.commit()
    scores2 = {int(r["id"]): r["centrality"] for r in conn.execute("SELECT id, centrality FROM memory_nodes")}
    assert scores == scores2


def test_build_graph_no_nodes(conn) -> None:
    result = build_graph(conn)
    assert result.relates_edges_created == 0
    assert result.nodes_scored == 0
