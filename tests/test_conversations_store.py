"""Store-level tests for the chat-history tables: additive migration, round-trip,
clear semantics, and the separability ablation (history never affects ask output)."""

from __future__ import annotations

import json

import pytest

from crossmodalrag.conversations.store import (
    clear_conversations,
    count_messages,
    create_conversation,
    derive_title,
    get_conversation,
    list_conversations,
    list_messages,
    record_message,
    touch_conversation,
)
from crossmodalrag.db import connect, init_db

T0 = "2026-07-11T10:00:00+00:00"
T1 = "2026-07-11T11:00:00+00:00"


@pytest.fixture
def conn(tmp_path):
    conn = connect(tmp_path / "memory.db")
    init_db(conn)
    yield conn
    conn.close()


def _seed_conversation(conn, *, started_at: str = T0) -> int:
    cid = create_conversation(conn, started_at=started_at, title="what is x?")
    record_message(conn, cid, turn_index=0, role="user", text="what is x?")
    record_message(
        conn,
        cid,
        turn_index=0,
        role="assistant",
        text="X is y [E1].",
        evidence_json=json.dumps([{"evidence_id": "E1", "chunk_id": 7, "source_uri": "/abs/x.md"}]),
        abstention_reason=None,
        truncated=False,
        model="stub-llm",
    )
    conn.commit()
    return cid


# --- additive migration ---------------------------------------------------------------


def test_init_db_is_additive_for_conversations(tmp_path):
    # The usage_events ablation pattern: dropping the history tables and
    # re-running init_db restores them without touching anything else.
    conn = connect(tmp_path / "memory.db")
    init_db(conn)
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", "/abs/a.md", T0, "a"),
    )
    conn.commit()
    source_id = int(cur.lastrowid)

    conn.execute("DROP TABLE messages")
    conn.execute("DROP TABLE conversations")
    conn.commit()
    init_db(conn)

    assert conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
    row = conn.execute("SELECT id, source_uri FROM sources").fetchone()
    assert (row["id"], row["source_uri"]) == (source_id, "/abs/a.md")
    conn.close()


# --- round-trip -------------------------------------------------------------------------


def test_round_trip_conversation_and_messages(conn):
    cid = _seed_conversation(conn)
    conv = get_conversation(conn, cid)
    assert conv is not None
    assert (conv.started_at, conv.updated_at, conv.title) == (T0, T0, "what is x?")

    messages = list_messages(conn, cid)
    assert [m.role for m in messages] == ["user", "assistant"]
    assert [m.turn_index for m in messages] == [0, 0]  # a question pairs with its answer
    assistant = messages[1]
    assert assistant.text == "X is y [E1]."
    assert assistant.model == "stub-llm"
    assert assistant.truncated is False
    assert json.loads(assistant.evidence_json)[0]["chunk_id"] == 7
    assert count_messages(conn, cid) == 2


def test_abstained_message_round_trips_reason(conn):
    cid = create_conversation(conn, started_at=T0, title="t")
    record_message(conn, cid, turn_index=0, role="user", text="unanswerable?")
    record_message(
        conn,
        cid,
        turn_index=0,
        role="assistant",
        text="Insufficient evidence…",
        abstention_reason="llm_insufficient",
    )
    conn.commit()
    assistant = list_messages(conn, cid)[1]
    assert assistant.abstention_reason == "llm_insufficient"
    assert assistant.evidence_json is None


def test_list_conversations_newest_first_and_touch(conn):
    c1 = _seed_conversation(conn, started_at=T0)
    c2 = _seed_conversation(conn, started_at=T1)
    assert [c.id for c in list_conversations(conn)] == [c2, c1]
    # Touching the older one promotes it.
    touch_conversation(conn, c1, updated_at="2026-07-11T12:00:00+00:00")
    conn.commit()
    assert [c.id for c in list_conversations(conn)] == [c1, c2]
    assert [c.id for c in list_conversations(conn, top=1)] == [c1]


def test_derive_title_collapses_and_truncates():
    assert derive_title("  what   is\nx? ") == "what is x?"
    long = "w" * 100
    title = derive_title(long, max_chars=80)
    assert len(title) == 81  # 80 + ellipsis
    assert title.endswith("…")


# --- clear ------------------------------------------------------------------------------


def test_clear_all_and_scoped(conn):
    c1 = _seed_conversation(conn)
    c2 = _seed_conversation(conn, started_at=T1)

    assert clear_conversations(conn, conversation_id=c1) == 1
    assert get_conversation(conn, c1) is None
    assert list_messages(conn, c1) == []
    assert count_messages(conn, c2) == 2  # untouched

    assert clear_conversations(conn) == 1
    assert list_conversations(conn) == []
    assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
    # Clearing a non-existent id clears nothing.
    assert clear_conversations(conn, conversation_id=999) == 0


# --- separability ablation ----------------------------------------------------------------


def test_ask_json_identical_with_history_populated_vs_dropped(tmp_path, monkeypatch, capsys):
    """The fingerprint-exclusion guarantee, end to end: `mem ask --json` output is
    identical with history populated vs the tables dropped (timing excluded)."""
    from crossmodalrag import cli

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
    _seed_conversation(conn)
    monkeypatch.setenv("CMRAG_DB_PATH", str(db_path))
    monkeypatch.setenv("CMRAG_MIN_EVIDENCE_SCORE", "0.0")

    def ask_payload() -> dict:
        cli.ask_cmd("parser bounds check", use_llm=False, as_json=True)
        payload = json.loads(capsys.readouterr().out)
        payload.pop("timing", None)
        return payload

    with_history = ask_payload()
    conn.execute("DROP TABLE messages")
    conn.execute("DROP TABLE conversations")
    conn.commit()
    conn.close()
    without_history = ask_payload()
    assert with_history == without_history
