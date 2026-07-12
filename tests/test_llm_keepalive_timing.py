from __future__ import annotations

import io
import json

import pytest

from crossmodalrag.config import get_llm_keep_alive
from crossmodalrag.generate import provider as provider_mod
from crossmodalrag.generate.answer import generated_answer_to_dict, template_answer_to_dict
from crossmodalrag.generate.provider import OllamaProvider
from crossmodalrag.generate.synthesize import synthesize_answer
from crossmodalrag.retrieve.lexical import RetrievalHit


def _hit(chunk_id: int = 1, text: str = "alpha", score: float = 0.9) -> RetrievalHit:
    return RetrievalHit(
        chunk_id=chunk_id,
        source_id=chunk_id,
        source_type="note",
        source_uri="/abs/note.md",
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
    def __init__(self, output: str = "Finding [E1].", name: str = "stub-llm") -> None:
        self.name = name
        self._output = output

    def generate(self, prompt: str, system: str | None = None) -> str:
        return self._output


# --- CMRAG_LLM_KEEP_ALIVE config parsing -------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param(None, "30m", id="default"),
        pytest.param("", "30m", id="empty-falls-back"),
        pytest.param("1h", "1h", id="duration-string"),
        pytest.param("600", 600.0, id="numeric-seconds"),
        pytest.param("-1", -1.0, id="pinned"),
    ],
)
def test_get_llm_keep_alive_parsing(
    monkeypatch: pytest.MonkeyPatch, raw: str | None, expected: float | str
) -> None:
    if raw is None:
        monkeypatch.delenv("CMRAG_LLM_KEEP_ALIVE", raising=False)
    else:
        monkeypatch.setenv("CMRAG_LLM_KEEP_ALIVE", raw)
    assert get_llm_keep_alive() == expected


# --- keep_alive lands in the Ollama request payload ---------------------------------------


def _capture_ollama_payload(monkeypatch: pytest.MonkeyPatch) -> dict:
    captured: dict = {}

    class _FakeResp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(json.dumps({"response": "ok"}).encode("utf-8"))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _fake_urlopen(request, timeout=None):
        captured.update(json.loads(request.data.decode("utf-8")))
        return _FakeResp()

    monkeypatch.setattr(provider_mod.urllib.request, "urlopen", _fake_urlopen)
    return captured


def test_ollama_payload_includes_configured_keep_alive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMRAG_LLM_KEEP_ALIVE", "45m")
    payload = _capture_ollama_payload(monkeypatch)
    OllamaProvider(model="x").generate("prompt")
    assert payload["keep_alive"] == "45m"


def test_ollama_payload_keep_alive_explicit_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMRAG_LLM_KEEP_ALIVE", "45m")
    payload = _capture_ollama_payload(monkeypatch)
    OllamaProvider(model="x", keep_alive=-1).generate("prompt")
    assert payload["keep_alive"] == -1


# --- timing block on the ask JSON contracts ------------------------------------------------


def test_generated_answer_timing_block_present_and_rounded() -> None:
    gen = synthesize_answer("q", [_hit()], StubLLMProvider(), min_evidence_score=0.0)
    data = generated_answer_to_dict(gen, total_seconds=1.23456)
    assert data["timing"] == {
        "total_seconds": 1.235,
        "generation_seconds": round(gen.generation_seconds, 3),
    }
    assert data["timing"]["generation_seconds"] >= 0.0


def test_generated_answer_timing_defaults_to_null_total() -> None:
    gen = synthesize_answer("q", [_hit()], StubLLMProvider(), min_evidence_score=0.0)
    data = generated_answer_to_dict(gen)
    assert data["timing"]["total_seconds"] is None


def test_gate_abstention_reports_zero_generation_seconds() -> None:
    # The weak-retrieval gate short-circuits before the LLM: no generation time to report.
    gen = synthesize_answer("q", [_hit(score=0.01)], StubLLMProvider(), min_evidence_score=0.5)
    assert gen.abstained
    assert gen.generation_seconds == 0.0
    assert generated_answer_to_dict(gen)["timing"]["generation_seconds"] == 0.0


def test_template_answer_timing_block() -> None:
    data = template_answer_to_dict("q", [_hit()], total_seconds=0.5)
    assert data["timing"] == {"total_seconds": 0.5, "generation_seconds": None}


def test_timing_is_additive_existing_keys_unchanged() -> None:
    gen = synthesize_answer("q", [_hit()], StubLLMProvider(), min_evidence_score=0.0)
    data = generated_answer_to_dict(gen, total_seconds=2.0)
    for key in (
        "query",
        "model",
        "abstained",
        "answer",
        "cited_evidence_ids",
        "invalid_citations",
        "evidence",
    ):
        assert key in data


# --- num_ctx (context window) lands in the Ollama request payload --------------------------
# Ollama's server default (4096) truncated evidence-heavy prompts (destroying the
# system prompt) and cut generation mid-answer (done_reason=length) — the
# empty/truncated-answer bug.


def test_ollama_payload_includes_default_num_ctx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CMRAG_LLM_NUM_CTX", raising=False)
    payload = _capture_ollama_payload(monkeypatch)
    OllamaProvider(model="x").generate("prompt")
    assert payload["options"]["num_ctx"] == 8192


def test_ollama_payload_num_ctx_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMRAG_LLM_NUM_CTX", "16384")
    payload = _capture_ollama_payload(monkeypatch)
    OllamaProvider(model="x").generate("prompt")
    assert payload["options"]["num_ctx"] == 16384


def test_ollama_payload_num_ctx_zero_defers_to_server(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CMRAG_LLM_NUM_CTX", "0")
    payload = _capture_ollama_payload(monkeypatch)
    OllamaProvider(model="x").generate("prompt")
    assert "num_ctx" not in payload["options"]


def test_get_llm_num_ctx_garbage_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    from crossmodalrag.config import get_llm_num_ctx

    monkeypatch.setenv("CMRAG_LLM_NUM_CTX", "lots")
    assert get_llm_num_ctx() == 8192


# --- done_reason=length surfaces as GeneratedAnswer.truncated ------------------------------


def _stream_response_lines(lines: list[dict]):
    body = "\n".join(json.dumps(obj) for obj in lines).encode("utf-8")

    class _FakeResp(io.BytesIO):
        def __init__(self) -> None:
            super().__init__(body)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    return _FakeResp


def test_provider_records_done_reason_length(monkeypatch: pytest.MonkeyPatch) -> None:
    resp = _stream_response_lines(
        [{"response": "partial ans", "done": False}, {"done": True, "done_reason": "length"}]
    )
    monkeypatch.setattr(
        provider_mod.urllib.request, "urlopen", lambda request, timeout=None: resp()
    )
    p = OllamaProvider(model="x")
    assert p.generate("prompt") == "partial ans"
    assert p.last_done_reason == "length"


def test_truncated_flag_and_warning_on_length_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    from crossmodalrag.generate.answer import format_generated_answer, generated_answer_to_dict
    from crossmodalrag.generate.synthesize import synthesize_answer

    resp = _stream_response_lines(
        [{"response": "cut off mid", "done": False}, {"done": True, "done_reason": "length"}]
    )
    monkeypatch.setattr(
        provider_mod.urllib.request, "urlopen", lambda request, timeout=None: resp()
    )
    gen = synthesize_answer("q", [_hit(1, "alpha")], OllamaProvider(model="x"), min_evidence_score=0.0)
    assert gen.truncated is True
    assert not gen.abstained
    rendered = format_generated_answer(gen)
    assert "cut off" in rendered and "CMRAG_LLM_NUM_CTX" in rendered
    assert generated_answer_to_dict(gen)["truncated"] is True


def test_normal_stop_is_not_truncated(monkeypatch: pytest.MonkeyPatch) -> None:
    from crossmodalrag.generate.answer import generated_answer_to_dict
    from crossmodalrag.generate.synthesize import synthesize_answer

    resp = _stream_response_lines(
        [{"response": "complete answer [E1]", "done": False}, {"done": True, "done_reason": "stop"}]
    )
    monkeypatch.setattr(
        provider_mod.urllib.request, "urlopen", lambda request, timeout=None: resp()
    )
    gen = synthesize_answer("q", [_hit(1, "alpha")], OllamaProvider(model="x"), min_evidence_score=0.0)
    assert gen.truncated is False
    assert generated_answer_to_dict(gen)["truncated"] is False
