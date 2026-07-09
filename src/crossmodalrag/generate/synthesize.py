from __future__ import annotations

import re
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass, field

from crossmodalrag.config import get_min_evidence_score
from crossmodalrag.generate.provider import LLMProvider
from crossmodalrag.modality import format_locator, parse_locator
from crossmodalrag.retrieve.lexical import RetrievalHit


CITATION_RE = re.compile(r"\[E(\d+)\]")

INSUFFICIENT_EVIDENCE_TEXT = (
    "Insufficient evidence in the current memory store to answer this question confidently."
)

SYSTEM_PROMPT = (
    "You are a knowledgeable assistant for a personal memory system. The numbered evidence "
    "below is the authoritative record of the user's own experience — their notes, code, and "
    "documents. Answer the question directly and insightfully, grounded in that experience.\n"
    "Every claim about the user's work, history, or records MUST cite its supporting evidence "
    "inline using the bracket ids exactly as shown, e.g. [E1] or [E2][E3].\n"
    "You may use general knowledge to explain, connect, and reason about the evidence, and to "
    "directly answer conceptual parts of the question that the evidence does not cover — but "
    "keep it subordinate: never present uncited knowledge as coming from the user's records, "
    "and never invent personal history.\n"
    "Consider EVERY evidence item before answering, and synthesize across ALL of the "
    "materially relevant ones — do not build the answer from a single item when several "
    "are relevant. When multiple items independently support a claim, cite each of them "
    "(e.g. [E1][E3]). Never cite an item that did not contribute to the answer.\n"
    "If the evidence only PARTIALLY addresses the question, answer what the evidence does "
    "support, fill remaining conceptual gaps from general knowledge (framed as such, never "
    "attributed to the records), and state explicitly what the records do not cover. A "
    "partial, clearly-caveated answer is always preferred over refusing — do not refuse "
    "just because coverage is incomplete.\n"
    "General knowledge alone never justifies a claim about the user's history. Only if NONE "
    "of the evidence is relevant to the question, respond with EXACTLY this "
    f"sentence and nothing else:\n{INSUFFICIENT_EVIDENCE_TEXT}"
)

# Why an answer abstained: the retrieval gate short-circuited before the LLM
# ("weak_retrieval") vs the model itself judged the evidence insufficient
# ("llm_insufficient"). The two used to render identically, which hid
# model-side over-refusal on well-retrieved evidence.
ABSTAIN_WEAK_RETRIEVAL = "weak_retrieval"
ABSTAIN_LLM_INSUFFICIENT = "llm_insufficient"


@dataclass(frozen=True)
class GeneratedAnswer:
    query: str
    answer_text: str
    cited_evidence_ids: list[str]
    invalid_citations: list[str]
    evidence: list[RetrievalHit]
    abstained: bool
    model: str
    raw_prompt: str = ""
    raw_output: str = ""
    id_map: dict[str, RetrievalHit] = field(default_factory=dict)
    # Wall-clock seconds spent inside the LLM call (0.0 when the gate abstained
    # before calling it). Surfaced via the `timing` JSON block so latency
    # regressions are measurable rather than anecdotal.
    generation_seconds: float = 0.0
    # None when answered; ABSTAIN_WEAK_RETRIEVAL / ABSTAIN_LLM_INSUFFICIENT when abstained.
    abstention_reason: str | None = None


def build_evidence_prompt(
    query: str, hits: list[RetrievalHit]
) -> tuple[str, str, dict[str, RetrievalHit]]:
    """Build the (system, prompt, id_map) for grounded synthesis.

    Each hit gets a stable ``[E#]`` id mapping back to its RetrievalHit so
    citations can be validated and rendered with full provenance.
    """
    id_map: dict[str, RetrievalHit] = {}
    lines: list[str] = ["Evidence:"]
    for idx, hit in enumerate(hits, start=1):
        eid = f"E{idx}"
        id_map[eid] = hit
        excerpt = " ".join(hit.chunk_text.split())
        locator = parse_locator(hit.chunk_metadata_json)
        modality = f"{locator.modality} | " if locator is not None and locator.modality else ""
        lines.append(
            f"[{eid}] ({hit.source_type}: {hit.title or 'untitled'} | {modality}"
            f"uri={format_locator(hit.source_uri, locator)} | chunk_id={hit.chunk_id})\n{excerpt}"
        )
    lines.append("")
    lines.append(f"Question: {query}")
    lines.append("Answer (cite evidence ids inline):")
    return SYSTEM_PROMPT, "\n".join(lines), id_map


def parse_citations(text: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for match in CITATION_RE.finditer(text):
        eid = f"E{match.group(1)}"
        if eid not in seen:
            seen.add(eid)
            ordered.append(eid)
    return ordered


def _gate_abstention(
    query: str, hits: list[RetrievalHit], provider: LLMProvider, min_evidence_score: float | None
) -> GeneratedAnswer | None:
    """The weak-retrieval gate: an abstention BEFORE calling the LLM, or None to proceed.

    Short-circuiting when there are no hits or the top hit scores below the
    threshold prevents ungrounded speculation (provenance-first /
    explicit-uncertainty non-negotiables).
    """
    threshold = min_evidence_score if min_evidence_score is not None else get_min_evidence_score()
    top_score = hits[0].score if hits else 0.0
    if hits and top_score >= threshold:
        return None
    return GeneratedAnswer(
        query=query,
        answer_text=INSUFFICIENT_EVIDENCE_TEXT,
        cited_evidence_ids=[],
        invalid_citations=[],
        evidence=hits,
        abstained=True,
        model=provider.name,
        abstention_reason=ABSTAIN_WEAK_RETRIEVAL,
    )


def _finalize_answer(
    query: str,
    hits: list[RetrievalHit],
    provider: LLMProvider,
    raw_output: str,
    prompt: str,
    id_map: dict[str, RetrievalHit],
    generation_seconds: float,
) -> GeneratedAnswer:
    """Citation validation + abstention detection over the complete model output."""
    cited = parse_citations(raw_output)
    valid = [eid for eid in cited if eid in id_map]
    invalid = [eid for eid in cited if eid not in id_map]
    abstained = raw_output.strip() == INSUFFICIENT_EVIDENCE_TEXT

    return GeneratedAnswer(
        query=query,
        answer_text=raw_output,
        cited_evidence_ids=valid,
        invalid_citations=invalid,
        evidence=hits,
        abstained=abstained,
        model=provider.name,
        raw_prompt=prompt,
        raw_output=raw_output,
        id_map=id_map,
        generation_seconds=generation_seconds,
        abstention_reason=ABSTAIN_LLM_INSUFFICIENT if abstained else None,
    )


def synthesize_answer_stream(
    query: str,
    hits: list[RetrievalHit],
    provider: LLMProvider,
    min_evidence_score: float | None = None,
) -> Generator[str, None, GeneratedAnswer]:
    """Streaming variant of :func:`synthesize_answer`: yields text fragments as the
    LLM produces them, then returns the finished ``GeneratedAnswer`` as the
    generator's return value (``StopIteration.value``).

    Citations/abstention are computed from the accumulated full output, so the
    returned answer is identical to the buffered path. The weak-retrieval gate
    abstains before the LLM, so a gated call yields no fragments. The library
    stays UI-agnostic — callers own any display of the fragments.
    """
    gated = _gate_abstention(query, hits, provider, min_evidence_score)
    if gated is not None:
        return gated

    system, prompt, id_map = build_evidence_prompt(query, hits)
    generation_start = time.monotonic()
    fragments: list[str] = []
    for fragment in provider.generate_stream(prompt, system=system):
        fragments.append(fragment)
        yield fragment
    raw_output = "".join(fragments).strip()
    generation_seconds = time.monotonic() - generation_start
    return _finalize_answer(query, hits, provider, raw_output, prompt, id_map, generation_seconds)


def synthesize_answer(
    query: str,
    hits: list[RetrievalHit],
    provider: LLMProvider,
    min_evidence_score: float | None = None,
    on_token: Callable[[str], None] | None = None,
) -> GeneratedAnswer:
    """Generate an evidence-constrained answer, abstaining when evidence is weak.

    Pass ``on_token`` to observe text fragments as the LLM produces them (via
    ``provider.generate_stream``); without it, the provider's buffered
    ``generate`` is used. Either way the ``GeneratedAnswer`` is built from the
    complete output, and ``on_token`` never fires for gate abstentions.
    """
    if on_token is not None:
        stream = synthesize_answer_stream(query, hits, provider, min_evidence_score)
        while True:
            try:
                fragment = next(stream)
            except StopIteration as stop:
                return stop.value
            on_token(fragment)

    gated = _gate_abstention(query, hits, provider, min_evidence_score)
    if gated is not None:
        return gated

    system, prompt, id_map = build_evidence_prompt(query, hits)
    generation_start = time.monotonic()
    raw_output = provider.generate(prompt, system=system)
    generation_seconds = time.monotonic() - generation_start
    return _finalize_answer(query, hits, provider, raw_output, prompt, id_map, generation_seconds)
