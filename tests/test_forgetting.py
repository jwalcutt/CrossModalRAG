from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.memory.forgetting import compute_forgetting_risk
from crossmodalrag.memory.store import add_edge, insert_node
from crossmodalrag.usage.store import record_usage_event

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)
OLD = "2026-01-01T00:00:00+00:00"
HALFLIFE = 30.0


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "memory.db")
    init_db(c)
    yield c
    c.close()


def _chunk(conn, uri="/n.md", text="evidence", ts: str | None = OLD) -> int:
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


def _node(conn, *, level=3, node_type="concept", title="c", time_end=OLD,
          centrality=None, chunk_ids=()) -> int:
    nid = insert_node(conn, level=level, node_type=node_type, title=title, time_end=time_end)
    if centrality is not None:
        conn.execute("UPDATE memory_nodes SET centrality = ? WHERE id = ?", (centrality, nid))
    for cid in chunk_ids:
        add_edge(conn, level, nid, 0, cid, "contains")
    conn.commit()
    return nid


def _risk_by_id(conn, **kw):
    items = compute_forgetting_risk(conn, now=NOW, halflife_days=HALFLIFE, **kw)
    return {it.node_id: it for it in items}


# --- ranking ------------------------------------------------------------------


def test_central_and_stale_outranks_fresh_and_low_central(conn):
    a = _node(conn, title="A central+stale", centrality=0.9, chunk_ids=[_chunk(conn, "/a.md")])
    b = _node(conn, title="B central+fresh", centrality=0.9, chunk_ids=[_chunk(conn, "/b.md")])
    c = _node(conn, title="C low-central", centrality=0.1, chunk_ids=[_chunk(conn, "/c.md")])
    # B was opened recently -> rehearsed -> low staleness.
    record_usage_event(conn, "node", b, "open", event_at="2026-06-30T00:00:00+00:00")
    conn.commit()

    ranked = compute_forgetting_risk(conn, now=NOW, halflife_days=HALFLIFE)
    order = [it.node_id for it in ranked]
    assert order[0] == a  # most at-risk: important and not revisited
    risks = {it.node_id: it.risk for it in ranked}
    assert risks[a] > risks[b]  # rehearsal lowered B's risk
    assert risks[a] > risks[c]  # higher importance than C


def test_recent_usage_lowers_risk(conn):
    n = _node(conn, title="n", centrality=0.8, chunk_ids=[_chunk(conn)])
    before = _risk_by_id(conn)[n].risk

    record_usage_event(conn, "node", n, "open", event_at="2026-06-29T00:00:00+00:00")
    conn.commit()
    after = _risk_by_id(conn)[n].risk

    assert after < before
    assert _risk_by_id(conn)[n].staleness < 0.5  # touched within ~a half-life


# --- grounding / support ------------------------------------------------------


def test_ungrounded_node_is_excluded(conn):
    grounded = _node(conn, title="g", centrality=0.5, chunk_ids=[_chunk(conn)])
    _node(conn, title="ungrounded", centrality=0.9, chunk_ids=[])  # no evidence

    ids = set(_risk_by_id(conn))
    assert grounded in ids
    assert len(ids) == 1  # the ungrounded high-centrality node never surfaces


def test_min_support_filter(conn):
    one = _node(conn, title="one", centrality=0.5, chunk_ids=[_chunk(conn, "/1.md")])
    three = _node(
        conn, title="three", centrality=0.5,
        chunk_ids=[_chunk(conn, "/x.md"), _chunk(conn, "/y.md"), _chunk(conn, "/z.md")],
    )

    ids = set(_risk_by_id(conn, min_support=2))
    assert three in ids and one not in ids


def test_confidence_rises_with_support(conn):
    one = _node(conn, title="one", centrality=0.5, chunk_ids=[_chunk(conn, "/1.md")])
    three = _node(
        conn, title="three", centrality=0.5,
        chunk_ids=[_chunk(conn, "/x.md"), _chunk(conn, "/y.md"), _chunk(conn, "/z.md")],
    )
    by_id = _risk_by_id(conn)
    assert by_id[one].confidence == pytest.approx(1 / 3)
    assert by_id[three].confidence == pytest.approx(1.0)


def test_grounding_and_components_in_range(conn):
    n = _node(conn, title="n", centrality=0.7, chunk_ids=[_chunk(conn)])
    item = _risk_by_id(conn)[n]
    assert item.evidence_source_uris  # traces to L0
    for v in (item.importance, item.staleness, item.risk, item.confidence):
        assert 0.0 <= v <= 1.0


# --- edge cases / determinism -------------------------------------------------


def test_no_time_info_is_maximally_stale(conn):
    # node has no time_end and its evidence source has no timestamp -> staleness 1.0
    cid = _chunk(conn, "/notime.md", ts=None)
    n = _node(conn, title="n", time_end=None, centrality=0.6, chunk_ids=[cid])
    item = _risk_by_id(conn)[n]
    assert item.staleness == 1.0
    assert item.risk == pytest.approx(0.6)


def test_centrality_absent_falls_back_to_support(conn):
    n = _node(conn, title="n", centrality=None,
              chunk_ids=[_chunk(conn, "/x.md"), _chunk(conn, "/y.md"), _chunk(conn, "/z.md")])
    assert _risk_by_id(conn)[n].importance == pytest.approx(1.0)  # support 3 / FULL 3


def test_deterministic_under_frozen_now(conn):
    _node(conn, title="a", centrality=0.9, chunk_ids=[_chunk(conn, "/a.md")])
    _node(conn, title="b", centrality=0.4, chunk_ids=[_chunk(conn, "/b.md")])
    first = [(it.node_id, round(it.risk, 6)) for it in compute_forgetting_risk(conn, now=NOW, halflife_days=HALFLIFE)]
    second = [(it.node_id, round(it.risk, 6)) for it in compute_forgetting_risk(conn, now=NOW, halflife_days=HALFLIFE)]
    assert first == second


def test_level_filter_selects_only_requested(conn):
    ev = _node(conn, level=1, node_type="event", title="ev", centrality=0.5, chunk_ids=[_chunk(conn, "/e.md")])
    co = _node(conn, level=3, node_type="concept", title="co", centrality=0.5, chunk_ids=[_chunk(conn, "/c.md")])
    assert set(_risk_by_id(conn, levels=(3,))) == {co}
    assert set(_risk_by_id(conn, levels=(1,))) == {ev}
    assert set(_risk_by_id(conn, levels=(1, 2, 3))) == {ev, co}
