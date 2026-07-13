"""`mem history` CLI tests: list/show/clear text + JSON shapes and error cases."""

from __future__ import annotations

import json
import sys

import pytest

from crossmodalrag import cli
from crossmodalrag.conversations.store import (
    create_conversation,
    record_message,
)
from crossmodalrag.db import connect, init_db

T0 = "2026-07-11T10:00:00+00:00"


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "load_dotenv", lambda *a, **k: None)
    db_path = tmp_path / "memory.db"
    conn = connect(db_path)
    init_db(conn)
    cid = create_conversation(conn, started_at=T0, title="what is x?")
    record_message(conn, cid, turn_index=0, role="user", text="what is x?")
    record_message(
        conn,
        cid,
        turn_index=0,
        role="assistant",
        text="X is y [E1].",
        evidence_json=json.dumps(
            [{"evidence_id": "E1", "cited": True, "chunk_id": 7, "source_uri": "/abs/x.md",
              "locator": "/abs/x.md"}]
        ),
        model="stub-llm",
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("CMRAG_DB_PATH", str(db_path))
    monkeypatch.delenv("CMRAG_SAVE_HISTORY", raising=False)
    return cid


def _run(monkeypatch, argv: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["mem", *argv])
    cli.main()


def test_history_list_json_shape(db, monkeypatch, capsys):
    _run(monkeypatch, ["history", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"save_enabled", "total", "conversations"}
    assert payload["save_enabled"] is True
    assert payload["total"] == 1
    conv = payload["conversations"][0]
    assert set(conv) == {"id", "started_at", "updated_at", "title", "message_count"}
    assert conv["message_count"] == 2


def test_history_show_json_shape(db, monkeypatch, capsys):
    _run(monkeypatch, ["history", "--show", str(db), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {"id", "started_at", "updated_at", "title", "message_count", "messages"}
    assert [m["role"] for m in payload["messages"]] == ["user", "assistant"]
    assistant = payload["messages"][1]
    assert set(assistant) == {
        "id", "role", "turn_index", "text", "abstention_reason", "truncated",
        "model", "created_at", "evidence",
    }
    assert assistant["evidence"][0]["evidence_id"] == "E1"
    assert payload["messages"][0]["evidence"] is None


def test_history_show_text_renders_exchange(db, monkeypatch, capsys):
    _run(monkeypatch, ["history", "--show", str(db)])
    out = capsys.readouterr().out
    assert 'Conversation #' in out
    assert "you> what is x?" in out
    assert "stub-llm> X is y [E1]." in out
    assert "[E1] /abs/x.md" in out  # cited-evidence ref line


def test_history_list_text(db, monkeypatch, capsys):
    _run(monkeypatch, ["history"])
    out = capsys.readouterr().out
    assert "what is x?" in out
    assert "(2 messages)" in out


def test_history_clear_all_and_scoped(db, monkeypatch, capsys):
    _run(monkeypatch, ["history", "--clear", "--id", "999", "--json"])
    assert json.loads(capsys.readouterr().out) == {"cleared": 0}
    _run(monkeypatch, ["history", "--clear", "--json"])
    assert json.loads(capsys.readouterr().out) == {"cleared": 1}
    _run(monkeypatch, ["history", "--json"])
    assert json.loads(capsys.readouterr().out)["total"] == 0


def test_history_show_unknown_id_errors(db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mem", "history", "--show", "424242"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "error:" in capsys.readouterr().err


def test_history_clear_and_show_conflict_errors(db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mem", "history", "--clear", "--show", "1"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "error:" in capsys.readouterr().err


def test_history_rename(db, monkeypatch, capsys):
    _run(monkeypatch, ["history", "--rename", str(db), "--title", "Parser deep dive", "--json"])
    assert json.loads(capsys.readouterr().out) == {"renamed": db, "title": "Parser deep dive"}
    _run(monkeypatch, ["history", "--show", str(db), "--json"])
    assert json.loads(capsys.readouterr().out)["title"] == "Parser deep dive"


def test_history_rename_requires_title(db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mem", "history", "--rename", str(db)])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "--title" in capsys.readouterr().err


def test_history_rename_conflicts_with_show(db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mem", "history", "--rename", "1", "--title", "x", "--show", "1"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "error:" in capsys.readouterr().err
