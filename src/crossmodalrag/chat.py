"""In-session conversation state for interactive ask (multi-turn chat).

Pure, UI-agnostic session logic: a bounded list of prior (question, answer)
turns plus a deterministic renderer that turns them into the optional
``history`` block consumed by ``generate.synthesize.build_evidence_prompt``.

Design invariants:

- Turns store the RAW answer text (including ``[E#]`` citation tokens) so a
  future persistence layer can keep full provenance; citations are stripped
  only at render time. Carried verbatim, stale ``[E#]`` ids from an earlier
  turn would collide with the CURRENT turn's evidence numbering and could be
  wrongly validated against it.
- Abstained turns are skipped entirely (question and answer): they contribute
  no grounded content, and carrying an orphan question invites the model to
  answer it ungrounded on a later turn — the exact behavior the retrieval
  gate exists to prevent.
- Eviction past ``max_turns`` is deterministic oldest-first; ``max_turns=0``
  disables carried context.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from crossmodalrag.config import get_chat_context_turns
from crossmodalrag.generate.synthesize import CITATION_RE

HISTORY_HEADER = (
    "Earlier conversation between you and the user. Use it to resolve follow-up "
    "references (\"that\", \"the last answer\") and to build on what was already "
    "said. Requests about the conversation itself (rephrase, summarize, continue) "
    "are answered from the conversation alone — do not refuse them for lack of "
    "evidence. The conversation is not evidence: never cite it; [E#] citations "
    "refer only to the numbered evidence below."
)


@dataclass(frozen=True)
class ChatTurn:
    query: str
    answer_text: str  # raw, including [E#] tokens; sanitized at render time


@dataclass
class ChatSession:
    max_turns: int = field(default_factory=get_chat_context_turns)
    turns: list[ChatTurn] = field(default_factory=list)

    def add_turn(self, query: str, answer_text: str, *, abstained: bool = False) -> None:
        """Record a completed turn; abstained turns are skipped entirely."""
        if abstained:
            return
        self.turns.append(ChatTurn(query=query, answer_text=answer_text))
        if len(self.turns) > self.max_turns:
            del self.turns[: len(self.turns) - self.max_turns]

    def clear(self) -> None:
        self.turns.clear()


def strip_citations(text: str) -> str:
    """Remove [E#] tokens (their ids referred to a previous turn's evidence)."""
    return CITATION_RE.sub("", text)


def render_history(turns: Sequence[ChatTurn]) -> str:
    """Deterministic history block for the evidence prompt; '' when no turns."""
    if not turns:
        return ""
    lines = [HISTORY_HEADER]
    for turn in turns:
        lines.append(f"User: {turn.query}")
        lines.append(f"Assistant: {strip_citations(turn.answer_text)}")
    return "\n".join(lines)
