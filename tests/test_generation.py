from __future__ import annotations

import json

import pytest

from crossmodalrag.db import connect, init_db
from crossmodalrag.generate import provider as provider_mod
from crossmodalrag.generate.answer import format_generated_answer, generated_answer_to_dict
from crossmodalrag.generate.provider import LLMUnavailable, OllamaProvider
from crossmodalrag.generate.synthesize import (
    INSUFFICIENT_EVIDENCE_TEXT,
    parse_citations,
    synthesize_answer,
)
from crossmodalrag.generation_eval import run_generation_eval
from crossmodalrag.retrieve.lexical import RetrievalHit


def _hit(chunk_id: int, text: str, uri: str = "/abs/note.md", score: float = 0.9) -> RetrievalHit:
    return RetrievalHit(
        chunk_id=chunk_id,
        source_id=chunk_id,
        source_type="note",
        source_uri=uri,
        source_timestamp="2026-06-01T00:00:00+00:00",
        title="note",
        chunk_index=0,
        chunk_text=text,
        score=score,
        lexical_score=score,
        recency_score=0.5,
        vector_score=0.0,
    )


class StubLLMProvider:
    def __init__(self, output: str = "Synthesized finding [E1].", name: str = "stub-llm") -> None:
        self.name = name
        self._output = output
        self.calls = 0

    def generate(self, prompt: str, system: str | None = None) -> str:
        self.calls += 1
        return self._output


def test_parse_citations_dedupes_and_orders() -> None:
    assert parse_citations("a [E2] b [E1] c [E2]") == ["E2", "E1"]


def test_synthesize_splits_valid_and_invalid_citations() -> None:
    hits = [_hit(1, "alpha"), _hit(2, "beta")]
    provider = StubLLMProvider(output="Claim one [E1]. Claim two [E5].")
    gen = synthesize_answer("q", hits, provider, min_evidence_score=0.0)
    assert gen.cited_evidence_ids == ["E1"]
    assert gen.invalid_citations == ["E5"]
    assert not gen.abstained


def test_weak_retrieval_gate_abstains_without_calling_llm() -> None:
    provider = StubLLMProvider()
    # Top score below threshold -> abstain before any generation.
    gen = synthesize_answer("q", [_hit(1, "alpha", score=0.05)], provider, min_evidence_score=0.15)
    assert gen.abstained
    assert gen.answer_text == INSUFFICIENT_EVIDENCE_TEXT
    assert provider.calls == 0


def test_no_hits_abstains() -> None:
    provider = StubLLMProvider()
    gen = synthesize_answer("q", [], provider)
    assert gen.abstained
    assert provider.calls == 0


def test_render_plain_text_includes_evidence_map() -> None:
    hits = [_hit(7, "evidence body")]
    gen = synthesize_answer("why", hits, StubLLMProvider(output="Because [E1]."), min_evidence_score=0.0)
    text = format_generated_answer(gen)
    assert "[E1]" in text
    assert "chunk_id=7" in text
    assert "Because [E1]." in text


def test_json_contract_invariants() -> None:
    hits = [_hit(7, "evidence body")]
    gen = synthesize_answer("why", hits, StubLLMProvider(output="Because [E1]."), min_evidence_score=0.0)
    data = generated_answer_to_dict(gen)
    assert data["model"] == "stub-llm"
    assert data["abstained"] is False
    assert data["evidence"][0]["evidence_id"] == "E1"
    assert data["evidence"][0]["cited"] is True
    # Round-trips as JSON (stable contract).
    json.dumps(data)


def test_ollama_provider_parses_response(monkeypatch) -> None:
    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({"response": "hello world"}).encode("utf-8")

    monkeypatch.setattr(provider_mod.urllib.request, "urlopen", lambda *a, **k: _FakeResp())
    assert OllamaProvider(model="x").generate("prompt") == "hello world"


def test_ollama_provider_raises_llm_unavailable(monkeypatch) -> None:
    def _boom(*a, **k):
        raise provider_mod.urllib.error.URLError("connection refused")

    monkeypatch.setattr(provider_mod.urllib.request, "urlopen", _boom)
    with pytest.raises(LLMUnavailable):
        OllamaProvider(model="x").generate("prompt")


def test_run_generation_eval_metrics(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CMRAG_MIN_EVIDENCE_SCORE", "0.0")
    conn = connect(tmp_path / "memory.db")
    init_db(conn)
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", "/abs/parser.md", "2026-06-01T00:00:00+00:00", "parser"),
    )
    sid = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (sid, 0, "parser bounds check fix details"),
    )
    # Answerable query (token overlap -> retrieved) and an unanswerable one (no overlap).
    conn.execute(
        "INSERT INTO queries_eval (query_text, expected_source_uris) VALUES (?, ?)",
        ("[test] parser bounds check", json.dumps(["/abs/parser.md"])),
    )
    conn.execute(
        "INSERT INTO queries_eval (query_text, expected_source_uris) VALUES (?, ?)",
        ("[test] zzzz unrelated tokyo revenue", json.dumps([])),
    )
    conn.commit()

    provider = StubLLMProvider(output="Grounded answer [E1].")
    summary = run_generation_eval(conn, provider, top_k=5, query_prefix="[test]")

    assert summary.query_count == 2
    assert summary.citation_validity == 1.0       # no hallucinated ids
    assert summary.source_grounding_hit == 1.0    # answerable cites the expected source
    assert summary.abstention_correct == 1.0      # answered the answerable, abstained the other


def test_ask_falls_back_to_template_when_llm_unavailable(tmp_path, monkeypatch, capsys) -> None:
    from crossmodalrag import cli

    db = tmp_path / "memory.db"
    monkeypatch.setenv("CMRAG_DB_PATH", str(db))
    conn = connect(db)
    init_db(conn)
    cur = conn.execute(
        "INSERT INTO sources (source_type, source_uri, timestamp, title) VALUES (?, ?, ?, ?)",
        ("note", "/abs/x.md", "2026-06-01T00:00:00+00:00", "x"),
    )
    conn.execute(
        "INSERT INTO evidence_chunks (source_id, chunk_index, chunk_text) VALUES (?, ?, ?)",
        (int(cur.lastrowid), 0, "fingerprint deterministic ingestion"),
    )
    conn.commit()
    conn.close()

    class _Raising:
        name = "raise-llm"

        def generate(self, prompt, system=None):
            raise LLMUnavailable("down")

    monkeypatch.setattr(cli, "get_default_llm_provider", lambda: _Raising())
    cli.ask_cmd("fingerprint", top_k=3)
    out = capsys.readouterr()
    assert "Evidence-grounded findings" in out.out
    assert "falling back" in out.err
