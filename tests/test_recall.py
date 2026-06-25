from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.generate.provider import LLMUnavailable
from crossmodalrag.memory.recall import generate_recall_cards
from crossmodalrag.memory.store import add_edge, delete_node, insert_node

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)
OLD = "2026-01-01T00:00:00+00:00"
HALFLIFE = 30.0


class StubLLM:
    def __init__(self, output: str | None = None, name: str = "stub-recall") -> None:
        self.name = name
        self._output = output or json.dumps({"question": "What is X?", "answer": "X is grounded."})
        self.calls = 0

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.calls += 1
        return self._output


class UnavailableLLM:
    name = "down"

    def __init__(self):
        self.calls = 0

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.calls += 1
        raise LLMUnavailable("connection refused")


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "memory.db")
    init_db(c)
    yield c
    c.close()


def _chunk(conn, uri="/n.md", text="evidence body about X", ts=OLD) -> int:
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", uri, ts, uri),
    )
    sid = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (sid, 0, text),
    )
    return int(cur.lastrowid)


def _node(conn, *, title="c", centrality=0.8, chunk_ids=()) -> int:
    nid = insert_node(conn, level=3, node_type="concept", title=title, time_end=OLD)
    conn.execute("UPDATE memory_nodes SET centrality = ? WHERE id = ?", (centrality, nid))
    for cid in chunk_ids:
        add_edge(conn, 3, nid, 0, cid, "contains")
    conn.commit()
    return nid


def _gen(conn, provider, **kw):
    return generate_recall_cards(conn, provider, now=NOW, halflife_days=HALFLIFE, **kw)


# --- generation + grounding + order -------------------------------------------


def test_generates_grounded_cards_in_risk_order(conn):
    high = _node(conn, title="high", centrality=0.9, chunk_ids=[_chunk(conn, "/h.md")])
    low = _node(conn, title="low", centrality=0.1, chunk_ids=[_chunk(conn, "/l.md")])
    cards = _gen(conn, StubLLM())
    assert [c.node_id for c in cards] == [high, low]  # risk order (centrality drives it)
    for c in cards:
        assert c.question == "What is X?"
        assert c.evidence_source_uris  # grounded to L0
        assert c.generated_by == "llm"


def test_additive_migration_recall_cards_present(conn):
    conn.execute("DROP TABLE recall_cards")
    conn.commit()
    init_db(conn)
    assert conn.execute("PRAGMA table_info(recall_cards)").fetchall()


# --- fingerprint-skip / regenerate --------------------------------------------


def test_fingerprint_skip_avoids_rellm_calls(conn):
    _node(conn, title="n", chunk_ids=[_chunk(conn)])
    provider = StubLLM()
    first = _gen(conn, provider)
    assert provider.calls == 1
    second = _gen(conn, provider)  # unchanged -> cached, no new call
    assert provider.calls == 1
    assert (second[0].question, second[0].answer) == (first[0].question, first[0].answer)


def test_regenerate_forces_recall(conn):
    _node(conn, title="n", chunk_ids=[_chunk(conn)])
    provider = StubLLM()
    _gen(conn, provider)
    _gen(conn, provider, regenerate=True)
    assert provider.calls == 2


def test_content_change_regenerates(conn):
    nid = _node(conn, title="n", chunk_ids=[_chunk(conn)])
    provider = StubLLM()
    _gen(conn, provider)
    assert provider.calls == 1
    conn.execute("UPDATE memory_nodes SET title = ? WHERE id = ?", ("n changed", nid))
    conn.commit()
    _gen(conn, provider)  # fingerprint changes -> regenerate
    assert provider.calls == 2


# --- fallback -----------------------------------------------------------------


def test_fallback_when_no_provider(conn):
    _node(conn, title="Smoke tests", chunk_ids=[_chunk(conn)])
    cards = _gen(conn, None)
    assert cards[0].generated_by == "fallback"
    assert "Smoke tests" in cards[0].question
    assert cards[0].answer  # grounded excerpt
    assert cards[0].evidence_source_uris


def test_fallback_when_llm_unavailable(conn):
    _node(conn, title="n", chunk_ids=[_chunk(conn)])
    provider = UnavailableLLM()
    cards = _gen(conn, provider)
    assert cards[0].generated_by == "fallback"
    assert provider.calls == 1  # tried once, then disabled for the run


# --- grounding guarantees / cleanup -------------------------------------------


def test_ungrounded_node_gets_no_card(conn):
    grounded = _node(conn, title="g", chunk_ids=[_chunk(conn)])
    _node(conn, title="ungrounded", chunk_ids=[])
    ids = {c.node_id for c in _gen(conn, StubLLM())}
    assert ids == {grounded}


def test_min_support_filter(conn):
    _node(conn, title="one", chunk_ids=[_chunk(conn, "/1.md")])
    three = _node(conn, title="three",
                  chunk_ids=[_chunk(conn, "/x.md"), _chunk(conn, "/y.md"), _chunk(conn, "/z.md")])
    ids = {c.node_id for c in _gen(conn, StubLLM(), min_support=2)}
    assert ids == {three}


def test_delete_node_removes_recall_card(conn):
    nid = _node(conn, title="n", chunk_ids=[_chunk(conn)])
    _gen(conn, StubLLM())
    assert conn.execute("SELECT COUNT(*) FROM recall_cards WHERE node_id = ?", (nid,)).fetchone()[0] == 1
    delete_node(conn, nid)
    assert conn.execute("SELECT COUNT(*) FROM recall_cards WHERE node_id = ?", (nid,)).fetchone()[0] == 0


def test_malformed_llm_output_falls_back(conn):
    _node(conn, title="n", chunk_ids=[_chunk(conn)])
    cards = _gen(conn, StubLLM(output="not json at all"))
    assert cards[0].generated_by == "fallback"
