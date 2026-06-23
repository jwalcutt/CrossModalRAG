from __future__ import annotations

import json

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.evaluation import run_eval
from crossmodalrag.memory.store import add_edge, insert_node
from crossmodalrag.retrieve.hybrid import retrieve


@pytest.fixture
def conn(tmp_path):
    connection = connect(tmp_path / "memory.db")
    init_db(connection)
    yield connection
    connection.close()


def _event(conn, title: str, uri: str) -> tuple[int, int]:
    cur = conn.execute("INSERT INTO sources (source_type, source_uri) VALUES ('note', ?)", (uri,))
    sid = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, 0, ?)", (sid, title)
    )
    chunk_id = int(cur.lastrowid)
    event_id = insert_node(conn, level=1, node_type="event", title=title)
    add_edge(conn, 1, event_id, 0, chunk_id, "derived_from")
    return event_id, chunk_id


def test_run_eval_concept_level_drilldown_recall(conn) -> None:
    e1, _ = _event(conn, "parser bounds check fix", "/abs/parser.md")
    concept = insert_node(conn, level=3, node_type="concept", title="parser bounds check fix")
    add_edge(conn, 3, concept, 1, e1, "contains")
    conn.execute(
        "INSERT INTO queries_eval (query_text, expected_source_uris) VALUES (?, ?)",
        ("[t] parser bounds", json.dumps(["/abs/parser.md"])),
    )
    conn.commit()

    # No embeddings -> node retrieval is lexical; drill-down should still recover the source.
    summary = run_eval(conn, top_k=5, query_prefix="[t]", level="concept")
    assert summary.query_count == 1
    assert summary.recall_at_k == 1.0


def test_restrict_chunk_ids_limits_results(conn) -> None:
    _e1, c1 = _event(conn, "parser bounds fix", "/abs/a.md")
    _e2, c2 = _event(conn, "parser bounds bug", "/abs/b.md")

    unrestricted = retrieve(conn, "parser bounds", top_k=5)
    assert {h.chunk_id for h in unrestricted} == {c1, c2}

    restricted = retrieve(conn, "parser bounds", top_k=5, restrict_chunk_ids={c1})
    assert {h.chunk_id for h in restricted} == {c1}
