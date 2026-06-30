"""Active-recall prompt generation (Phase 4 step 5).

Turn the top forgetting-risk memories into grounded study cards (a question + a
one-sentence, evidence-grounded answer). Mirrors the L1-extraction discipline
(local LLM, temp 0, versioned prompt, fingerprint-skip cache) and the concept-naming
graceful fallback (LLM → on LLMUnavailable disable for the run → deterministic template).

Cards are a derived cache (`recall_cards`); they are never part of any content/derivation
fingerprint, so determinism/idempotency of ingestion + the memory hierarchy are unaffected.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from crossmodalrag.generate.provider import LLMProvider, LLMUnavailable
from crossmodalrag.memory.forgetting import ForgettingRisk, compute_forgetting_risk
from crossmodalrag.memory.store import resolve_to_evidence

RECALL_PROMPT_VERSION = "recall-v1"
MAX_EVIDENCE_CHARS = 4000

RECALL_SYSTEM_PROMPT = (
    "You write ONE active-recall study question from a memory and its supporting evidence.\n"
    "Respond with ONLY a JSON object (no prose, no code fences) with keys:\n"
    '  "question": a single question that tests understanding of the memory\n'
    '  "answer": one sentence answering it, grounded STRICTLY in the provided evidence '
    "(do not use outside knowledge).\n"
    "If the evidence is too thin to answer, make the answer say so."
)


@dataclass(frozen=True)
class RecallCard:
    node_id: int
    level: int
    node_type: str
    title: str | None
    question: str
    answer: str | None
    risk: float
    confidence: float
    evidence_source_uris: list[str]
    generated_by: str  # "llm" | "fallback"


def recall_card_to_dict(card: RecallCard) -> dict:
    """Stable JSON contract for `mem recall --json`. Keep field names backward-compatible."""
    return {
        "node_id": card.node_id,
        "level": card.level,
        "node_type": card.node_type,
        "title": card.title,
        "question": card.question,
        "answer": card.answer,
        "risk": card.risk,
        "confidence": card.confidence,
        "generated_by": card.generated_by,
        "evidence_source_uris": list(card.evidence_source_uris),
    }


def generate_recall_cards(
    conn: sqlite3.Connection,
    provider: LLMProvider | None,
    *,
    now: datetime,
    halflife_days: float,
    levels: tuple[int, ...] = (3,),
    top: int = 10,
    min_support: int = 1,
    regenerate: bool = False,
) -> list[RecallCard]:
    """Generate/load grounded recall cards for the top forgetting-risk nodes (risk order)."""
    items = compute_forgetting_risk(
        conn,
        now=now,
        halflife_days=halflife_days,
        levels=levels,
        min_support=min_support,
        top=top,
    )

    cards: list[RecallCard] = []
    llm_enabled = provider is not None
    for item in items:
        evidence_text = _node_evidence_text(conn, item.level, item.node_id)
        fingerprint = _fingerprint(item, evidence_text)

        cached = None if regenerate else _get_card(conn, item.node_id)
        if cached is not None and cached["fingerprint"] == fingerprint:
            question, answer, generated_by = (
                str(cached["question"]),
                cached["answer"],
                str(cached["generated_by"]),
            )
        else:
            question = answer = None
            model = "fallback"
            if llm_enabled:
                try:
                    question, answer = _llm_generate(provider, item, evidence_text)
                    model = provider.name
                except LLMUnavailable:
                    llm_enabled = False  # stop retrying for the rest of this run
            generated_by = "llm" if question is not None else "fallback"
            if question is None:
                question, answer = _fallback_card(item, evidence_text)
            _upsert_card(conn, item.node_id, question, answer, fingerprint, model, generated_by)

        cards.append(
            RecallCard(
                node_id=item.node_id,
                level=item.level,
                node_type=item.node_type,
                title=item.title,
                question=question,
                answer=answer,
                risk=item.risk,
                confidence=item.confidence,
                evidence_source_uris=item.evidence_source_uris,
                generated_by=generated_by,
            )
        )

    conn.commit()
    return cards


def _llm_generate(
    provider: LLMProvider, item: ForgettingRisk, evidence_text: str
) -> tuple[str | None, str | None]:
    title = item.title or "this memory"
    prompt = (
        f"Memory: {title}\n\nEvidence:\n{evidence_text}\n\n"
        "Write the active-recall card as a JSON object:"
    )
    raw = provider.generate(prompt, system=RECALL_SYSTEM_PROMPT)
    parsed = _parse_card(raw)
    if parsed is None:
        return None, None  # unparseable -> fall back
    return parsed


def _fallback_card(item: ForgettingRisk, evidence_text: str) -> tuple[str, str | None]:
    title = item.title or "this memory"
    question = f"What do you remember about: {title}?"
    answer = _preview(evidence_text) if evidence_text.strip() else None
    return question, answer


def _parse_card(raw: str) -> tuple[str, str | None] | None:
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    try:
        obj = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    question = str(obj.get("question", "")).strip()
    if not question:
        return None
    answer = str(obj.get("answer", "")).strip() or None
    return question, answer


def _node_evidence_text(conn: sqlite3.Connection, level: int, node_id: int) -> str:
    chunk_ids = resolve_to_evidence(conn, level, node_id)
    if not chunk_ids:
        return ""
    placeholders = ",".join("?" for _ in chunk_ids)
    rows = conn.execute(
        f"SELECT chunk_text FROM evidence_chunks WHERE id IN ({placeholders}) ORDER BY id ASC",
        tuple(chunk_ids),
    ).fetchall()
    return "\n\n".join(str(r["chunk_text"]) for r in rows)[:MAX_EVIDENCE_CHARS]


def _fingerprint(item: ForgettingRisk, evidence_text: str) -> str:
    # Content-only (excludes model): a card is reused while its memory is unchanged, so a transient
    # Ollama outage never downgrades a cached LLM card. `--regenerate` upgrades fallbacks later.
    payload = f"{RECALL_PROMPT_VERSION}\x1f{item.title or ''}\x1f{evidence_text}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _get_card(conn: sqlite3.Connection, node_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM recall_cards WHERE node_id = ?", (node_id,)).fetchone()


def _upsert_card(
    conn: sqlite3.Connection,
    node_id: int,
    question: str,
    answer: str | None,
    fingerprint: str,
    model: str,
    generated_by: str,
) -> None:
    conn.execute(
        """
        INSERT INTO recall_cards (node_id, question, answer, fingerprint, model, prompt_version, generated_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            question = excluded.question,
            answer = excluded.answer,
            fingerprint = excluded.fingerprint,
            model = excluded.model,
            prompt_version = excluded.prompt_version,
            generated_by = excluded.generated_by,
            created_at = CURRENT_TIMESTAMP
        """,
        (node_id, question, answer, fingerprint, model, RECALL_PROMPT_VERSION, generated_by),
    )


def _preview(text: str, max_chars: int = 220) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= max_chars else f"{flat[:max_chars].rstrip()}…"
