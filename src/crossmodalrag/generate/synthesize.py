from __future__ import annotations

import re
import time
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
    "You are a careful, evidence-grounded assistant for a personal memory system.\n"
    "Answer ONLY using the numbered evidence provided. Do not use any outside knowledge.\n"
    "Every claim in your answer MUST cite its supporting evidence inline using the bracket "
    "ids exactly as shown, e.g. [E1] or [E2][E3].\n"
    "If the evidence only PARTIALLY addresses the question, answer what the evidence does "
    "support and state explicitly what it does not cover. A partial, clearly-caveated answer "
    "is always preferred over refusing — do not refuse just because coverage is incomplete.\n"
    "Only if NONE of the evidence is relevant to the question, respond with EXACTLY this "
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


def synthesize_answer(
    query: str,
    hits: list[RetrievalHit],
    provider: LLMProvider,
    min_evidence_score: float | None = None,
) -> GeneratedAnswer:
    """Generate an evidence-constrained answer, abstaining when evidence is weak.

    The weak-retrieval gate short-circuits BEFORE calling the LLM when there are
    no hits or the top hit scores below the threshold, preventing ungrounded
    speculation (provenance-first / explicit-uncertainty non-negotiables).
    """
    threshold = min_evidence_score if min_evidence_score is not None else get_min_evidence_score()
    top_score = hits[0].score if hits else 0.0
    if not hits or top_score < threshold:
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

    system, prompt, id_map = build_evidence_prompt(query, hits)
    generation_start = time.monotonic()
    raw_output = provider.generate(prompt, system=system)
    generation_seconds = time.monotonic() - generation_start

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
