"""Rebuild in-session chat context from a stored conversation (resume)."""

from __future__ import annotations

from crossmodalrag.chat import ChatTurn
from crossmodalrag.conversations.store import MessageRow


def turns_from_messages(messages: list[MessageRow]) -> list[ChatTurn]:
    """Stored messages → context turns, applying the session's context rules.

    Pairs each user question with its assistant answer by ``turn_index`` and
    skips abstained turns entirely (the ``ChatSession.add_turn`` invariant: an
    orphan/refused exchange must not tempt the model into ungrounded answers).
    The caller feeds these through ``ChatSession.add_turn`` so the context cap
    still applies.
    """
    questions: dict[int, str] = {}
    turns: list[ChatTurn] = []
    for message in messages:  # already ordered by insertion (id ASC)
        if message.role == "user":
            questions[message.turn_index] = message.text
            continue
        if message.abstention_reason is not None:
            continue
        query = questions.get(message.turn_index)
        if query is None:
            continue  # defensive: an assistant row without its question
        turns.append(ChatTurn(query=query, answer_text=message.text))
    return turns


def next_turn_index(messages: list[MessageRow]) -> int:
    """The turn_index new (resumed) turns should continue from."""
    return max((m.turn_index for m in messages), default=-1) + 1
