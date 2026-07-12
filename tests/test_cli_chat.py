"""REPL tests for the interactive multi-turn session (`mem chat` / bare `mem ask`),
driven through `cli.main()` with faked stdin and a recording stub provider."""

from __future__ import annotations

import os
import sys

import pytest

from crossmodalrag import cli
from crossmodalrag.chat import HISTORY_HEADER
from crossmodalrag.db import connect, init_db
from crossmodalrag.generate.synthesize import INSUFFICIENT_EVIDENCE_TEXT

CONNECTOR_PREFIXES = ("OBSIDIAN_VAULT_PATH", "REPO_PATH", "PDF_PATH", "IMAGE_PATH")


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A small DB (one note + one matching chunk) wired as the active CMRAG_DB_PATH,
    with connector env vars cleared and `.env` reloading disabled."""
    for key in list(os.environ):
        if any(key.startswith(f"{p}_") for p in CONNECTOR_PREFIXES):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(cli, "load_dotenv", lambda *a, **k: None)
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
    monkeypatch.delenv("CMRAG_USAGE_TRACKING", raising=False)
    monkeypatch.delenv("CMRAG_CHAT_CONTEXT_TURNS", raising=False)
    monkeypatch.setenv("CMRAG_MIN_EVIDENCE_SCORE", "0.0")
    return db_path


class RecordingProvider:
    """Captures every (prompt, system) pair; returns a fixed answer."""

    def __init__(self, output: str = "Grounded claim [E1].", name: str = "stub-llm") -> None:
        self.name = name
        self._output = output
        self.prompts: list[str] = []

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.prompts.append(prompt)
        return self._output

    def generate_stream(self, prompt: str, system: str | None = None):
        self.prompts.append(prompt)
        yield self._output


def _feed_input(monkeypatch, lines: list[str]) -> None:
    it = iter(lines)

    def fake_input(prompt: str = "") -> str:
        try:
            return next(it)
        except StopIteration:
            raise EOFError

    monkeypatch.setattr("builtins.input", fake_input)


def _run(monkeypatch, argv: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["mem", *argv])
    cli.main()


def _history_block(prompt: str) -> str:
    """The part of a turn prompt before the current turn's evidence."""
    return prompt.split("Evidence:")[0]


# --- multi-turn context threading -----------------------------------------------------


def test_second_turn_prompt_carries_first_turn(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["what changed in the parser?", "expand on the parser fix"])
    _run(monkeypatch, ["chat"])
    assert len(provider.prompts) == 2
    assert HISTORY_HEADER not in provider.prompts[0]
    block = _history_block(provider.prompts[1])
    assert HISTORY_HEADER in block
    assert "User: what changed in the parser?" in block
    assert "Grounded claim" in block


def test_stale_citations_stripped_from_history(db, monkeypatch):
    provider = RecordingProvider(output="Claim [E1]. More [E2].")
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser first question", "parser second question"])
    _run(monkeypatch, ["chat"])
    block = _history_block(provider.prompts[1])
    assert "Claim" in block
    # Stale evidence ids never reach the next prompt (the header's literal
    # "[E#]" is instructional text, not a citation id).
    assert "[E1]" not in block
    assert "[E2]" not in block


def test_bare_ask_enters_the_same_session(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1", "parser q2"])
    _run(monkeypatch, ["ask"])
    assert HISTORY_HEADER in provider.prompts[1]


# --- session commands ------------------------------------------------------------------


def test_clear_resets_context(db, monkeypatch, capsys):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1", "/clear", "parser q2"])
    _run(monkeypatch, ["chat"])
    assert HISTORY_HEADER not in provider.prompts[1]
    assert "[context cleared]" in capsys.readouterr().out


def test_new_alias_resets_context(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1", "/new", "parser q2"])
    _run(monkeypatch, ["chat"])
    assert HISTORY_HEADER not in provider.prompts[1]


def test_exit_stops_before_later_lines(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1", "/exit", "never asked"])
    _run(monkeypatch, ["chat"])
    assert len(provider.prompts) == 1


def test_blank_lines_ignored(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["", "   ", "parser q1"])
    _run(monkeypatch, ["chat"])
    assert len(provider.prompts) == 1


def test_eof_and_keyboard_interrupt_exit_cleanly(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    # EOF: input exhaustion (the _feed_input default).
    _feed_input(monkeypatch, ["parser q1"])
    _run(monkeypatch, ["chat"])  # no SystemExit, no traceback

    # Ctrl-C at the prompt.
    def raise_interrupt(prompt: str = "") -> str:
        raise KeyboardInterrupt

    monkeypatch.setattr("builtins.input", raise_interrupt)
    _run(monkeypatch, ["chat"])  # swallowed by the loop's handler


# --- provenance / gating per turn -------------------------------------------------------


def test_abstained_turn_not_carried_into_history(db, monkeypatch):
    provider = RecordingProvider()
    outputs = iter([INSUFFICIENT_EVIDENCE_TEXT, "Grounded claim [E1]."])

    def gen(prompt: str, system: str | None = None) -> str:
        provider.prompts.append(prompt)
        return next(outputs)

    monkeypatch.setattr(provider, "generate", gen)
    monkeypatch.setattr(
        provider, "generate_stream", lambda prompt, system=None: iter([gen(prompt, system)])
    )
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser unanswerable?", "parser answerable?"])
    _run(monkeypatch, ["chat"])
    assert HISTORY_HEADER not in provider.prompts[1]


def test_weak_retrieval_gate_fires_per_turn(db, monkeypatch, capsys):
    monkeypatch.setenv("CMRAG_MIN_EVIDENCE_SCORE", "99.0")  # nothing can pass
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["q1", "q2"])
    _run(monkeypatch, ["chat"])
    assert provider.prompts == []  # gate short-circuits before the LLM every turn
    out = capsys.readouterr().out
    assert out.count(INSUFFICIENT_EVIDENCE_TEXT) == 2
    # And nothing was carried: both turns abstained.


def test_context_cap_evicts_oldest_end_to_end(db, monkeypatch):
    monkeypatch.setenv("CMRAG_CHAT_CONTEXT_TURNS", "1")
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q-one", "parser q-two", "parser q-three"])
    _run(monkeypatch, ["chat"])
    block = _history_block(provider.prompts[2])
    assert "User: parser q-two" in block
    assert "q-one" not in block


# --- one-shot back-compat ---------------------------------------------------------------


def test_one_shot_ask_never_reads_stdin(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    monkeypatch.setattr(
        "builtins.input", lambda *a: pytest.fail("one-shot ask must not read stdin")
    )
    _run(monkeypatch, ["ask", "what changed in the parser?"])
    assert len(provider.prompts) == 1
    assert HISTORY_HEADER not in provider.prompts[0]


def test_json_without_query_is_an_error(db, monkeypatch, capsys):
    _feed_input(monkeypatch, [])
    monkeypatch.setattr(sys, "argv", ["mem", "ask", "--json"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "error:" in capsys.readouterr().err


def test_accept_without_query_is_an_error(db, monkeypatch, capsys):
    _feed_input(monkeypatch, [])
    monkeypatch.setattr(sys, "argv", ["mem", "ask", "--accept"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1
    assert "error:" in capsys.readouterr().err


# --- per-turn usage tracking -------------------------------------------------------------


def test_tracking_records_each_turn(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    calls: list[dict] = []
    monkeypatch.setattr(cli, "_track_ask", lambda db_path, **kw: calls.append(kw))
    _feed_input(monkeypatch, ["parser q1", "parser q2"])
    _run(monkeypatch, ["chat", "--track"])
    assert len(calls) == 2


def test_no_track_records_nothing(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    calls: list[dict] = []
    monkeypatch.setattr(cli, "_track_ask", lambda db_path, **kw: calls.append(kw))
    _feed_input(monkeypatch, ["parser q1", "parser q2"])
    _run(monkeypatch, ["chat", "--no-track"])
    assert calls == []


# --- conversation persistence (step 12) ---------------------------------------------------


def _history_rows(db_path):
    from crossmodalrag.db import connect

    conn = connect(db_path)
    try:
        convs = conn.execute("SELECT id, title FROM conversations ORDER BY id").fetchall()
        msgs = conn.execute(
            "SELECT conversation_id, role, text, abstention_reason FROM messages ORDER BY id"
        ).fetchall()
        return [tuple(c) for c in convs], [tuple(m) for m in msgs]
    finally:
        conn.close()


def test_chat_session_persists_turns(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1", "parser q2"])
    _run(monkeypatch, ["chat"])
    convs, msgs = _history_rows(db)
    assert len(convs) == 1
    assert convs[0][1] == "parser q1"  # title from first query
    assert [(m[1], m[2]) for m in msgs] == [
        ("user", "parser q1"),
        ("assistant", "Grounded claim [E1]."),
        ("user", "parser q2"),
        ("assistant", "Grounded claim [E1]."),
    ]


def test_new_rotates_conversation_clear_does_not(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1", "/clear", "parser q2", "/new", "parser q3"])
    _run(monkeypatch, ["chat"])
    convs, msgs = _history_rows(db)
    assert len(convs) == 2  # /clear stayed in conversation 1; /new opened 2
    by_conv = {}
    for conversation_id, role, text, _reason in msgs:
        if role == "user":
            by_conv.setdefault(conversation_id, []).append(text)
    assert sorted(by_conv.values()) == [["parser q1", "parser q2"], ["parser q3"]]


def test_abstained_turn_is_persisted_with_reason(db, monkeypatch):
    provider = RecordingProvider(output=INSUFFICIENT_EVIDENCE_TEXT)
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1"])
    _run(monkeypatch, ["chat"])
    _convs, msgs = _history_rows(db)
    assert [(m[1], m[3]) for m in msgs] == [("user", None), ("assistant", "llm_insufficient")]


def test_no_save_flag_persists_nothing(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1"])
    _run(monkeypatch, ["chat", "--no-save"])
    convs, msgs = _history_rows(db)
    assert convs == [] and msgs == []


def test_save_history_env_off_persists_nothing(db, monkeypatch):
    monkeypatch.setenv("CMRAG_SAVE_HISTORY", "off")
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _feed_input(monkeypatch, ["parser q1"])
    _run(monkeypatch, ["chat"])
    convs, msgs = _history_rows(db)
    assert convs == [] and msgs == []


def test_one_shot_ask_never_persists(db, monkeypatch):
    provider = RecordingProvider()
    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: provider)
    _run(monkeypatch, ["ask", "parser q1"])
    convs, msgs = _history_rows(db)
    assert convs == [] and msgs == []
