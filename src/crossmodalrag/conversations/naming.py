"""LLM-generated conversation titles.

A conversation is named once, from its first exchange (the observable context),
by the configured local LLM. Deterministic (the provider runs at temperature 0),
short (hard-clamped to a few words), and strictly best-effort: any failure —
Ollama down, empty output — yields ``None`` and the caller falls back to the
deterministic first-query title (``store.derive_title``).
"""

from __future__ import annotations

import re

from crossmodalrag.generate.provider import LLMProvider, LLMUnavailable

TITLE_MAX_WORDS = 7
TITLE_MAX_CHARS = 60

_TITLE_SYSTEM = (
    "You title conversations for a personal memory app. Reply with ONLY the "
    "title: at most 6 words, plain text, no quotes, no trailing punctuation, "
    "no explanations."
)


def generate_conversation_title(
    provider: LLMProvider, *, query: str, answer_text: str
) -> str | None:
    """A short LLM-generated title for the conversation's first exchange, or None."""
    prompt = (
        "Title this conversation.\n"
        f"User: {_clip(query, 300)}\n"
        f"Assistant: {_clip(answer_text, 400)}\n"
        "Title:"
    )
    try:
        raw = provider.generate(prompt, system=_TITLE_SYSTEM)
    except LLMUnavailable:
        return None
    title = sanitize_title(raw)
    return title or None


def sanitize_title(raw: str) -> str:
    """First line only, unquoted, unpunctuated tail, clamped to a few words."""
    line = raw.strip().splitlines()[0].strip() if raw.strip() else ""
    line = line.strip("\"'`“”‘’")
    line = re.sub(r"^[Tt]itle\s*:\s*", "", line).strip()
    words = line.split()
    if len(words) > TITLE_MAX_WORDS:
        words = words[:TITLE_MAX_WORDS]
    title = " ".join(words).rstrip(".,;:!?-–—")
    if len(title) > TITLE_MAX_CHARS:
        title = title[:TITLE_MAX_CHARS].rstrip() + "…"
    return title


def _clip(text: str, max_chars: int) -> str:
    collapsed = " ".join(text.split())
    return collapsed[:max_chars]
