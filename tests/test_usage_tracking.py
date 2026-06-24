from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crossmodalrag import cli
from crossmodalrag.db import connect, init_db
from crossmodalrag.usage.store import list_usage_events
from crossmodalrag.usage.tracking import record_ask_interaction

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A small DB (one note + one matching chunk) wired as the active CMRAG_DB_PATH."""
    db_path = tmp_path / "memory.db"
    conn = connect(db_path)
    init_db(conn)
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", "/abs/parser.md", "2026-06-01T00:00:00+00:00", "parser"),
    )
    sid = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (sid, 0, "parser bounds check off by one bug fix"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("CMRAG_DB_PATH", str(db_path))
    monkeypatch.delenv("CMRAG_USAGE_TRACKING", raising=False)  # default off
    return db_path


def _events(db_path):
    conn = connect(db_path)
    try:
        return list_usage_events(conn)
    finally:
        conn.close()


# --- pure helper --------------------------------------------------------------


def test_record_ask_interaction_writes_each_event_type(tmp_path):
    conn = connect(tmp_path / "m.db")
    init_db(conn)
    n = record_ask_interaction(
        conn,
        now=NOW,
        retrieved_chunk_ids=[1, 2, 2],  # de-duped
        accepted_chunk_ids=[1],
        opened_node_ids=[9],
    )
    events = list_usage_events(conn)
    conn.close()
    assert n == 4
    kinds = sorted((e.target_kind, e.event_type, e.target_id) for e in events)
    assert kinds == [
        ("chunk", "accepted_answer", 1),
        ("chunk", "retrieval_hit", 1),
        ("chunk", "retrieval_hit", 2),
        ("node", "open", 9),
    ]


# --- ask_cmd integration (no Ollama: use_llm=False) ---------------------------


def test_ask_default_off_records_nothing(db):
    cli.ask_cmd("parser bounds check", use_llm=False)
    assert _events(db) == []


def test_ask_track_records_retrieval_hit(db):
    cli.ask_cmd("parser bounds check", use_llm=False, track=True)
    events = _events(db)
    assert events
    assert all(e.event_type == "retrieval_hit" and e.target_kind == "chunk" for e in events)


def test_ask_accept_records_accepted_answer_even_with_tracking_off(db):
    cli.ask_cmd("parser bounds check", use_llm=False, accept=True)
    types = {e.event_type for e in _events(db)}
    assert "accepted_answer" in types
    assert "retrieval_hit" in types


def test_no_track_suppresses_even_when_env_on(db, monkeypatch):
    monkeypatch.setenv("CMRAG_USAGE_TRACKING", "on")
    cli.ask_cmd("parser bounds check", use_llm=False, track=False)
    assert _events(db) == []


def test_tracking_failure_never_breaks_ask(db, monkeypatch, capsys):
    def _boom(*a, **k):
        raise RuntimeError("disk full")

    monkeypatch.setattr("crossmodalrag.usage.tracking.record_ask_interaction", _boom)
    # Should not raise; prints a notice and still answers.
    cli.ask_cmd("parser bounds check", use_llm=False, track=True)
    assert _events(db) == []
    assert "usage tracking skipped" in capsys.readouterr().err


def test_privacy_no_query_text_persisted(db):
    cli.ask_cmd("a very distinctive secret query phrase parser", use_llm=False, track=True)
    # usage_events has no column that could hold the query, and none of the stored values do.
    conn = connect(db)
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(usage_events)").fetchall()}
        rows = conn.execute("SELECT * FROM usage_events").fetchall()
    finally:
        conn.close()
    assert "query" not in " ".join(cols).lower()
    blob = " ".join(str(v) for r in rows for v in tuple(r))
    assert "distinctive secret" not in blob


# --- mem usage command --------------------------------------------------------


def test_usage_cmd_clear_wipes(db, capsys):
    cli.ask_cmd("parser bounds check", use_llm=False, track=True)
    assert _events(db)
    cli.usage_cmd(clear=True)
    assert "Cleared" in capsys.readouterr().out
    assert _events(db) == []


def test_usage_cmd_stats_prints(db, capsys):
    cli.ask_cmd("parser bounds check", use_llm=False, track=True)
    cli.usage_cmd()
    out = capsys.readouterr().out
    assert "Total usage events:" in out
    assert "retrieval_hit" in out
