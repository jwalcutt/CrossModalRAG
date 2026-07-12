"""Stable JSON contracts for chat history (the ``*_to_dict`` convention).

Additive-only, no version field (the ``generated_answer_to_dict`` precedent).
These shapes are what `mem history --json` emits and what the local API's
conversation endpoints will return verbatim.
"""

from __future__ import annotations

import json
import sqlite3

from crossmodalrag.conversations.store import (
    ConversationRow,
    MessageRow,
    count_messages,
    list_messages,
)


def message_to_dict(message: MessageRow) -> dict:
    """One message. ``evidence`` is the stored point-in-time ledger snapshot
    (same element shape as the ask contract's ``evidence`` array) or ``None``
    for user messages."""
    return {
        "id": message.id,
        "role": message.role,
        "turn_index": message.turn_index,
        "text": message.text,
        "abstention_reason": message.abstention_reason,
        "truncated": message.truncated,
        "model": message.model,
        "created_at": message.created_at,
        "evidence": json.loads(message.evidence_json) if message.evidence_json else None,
    }


def conversation_to_dict(
    conn: sqlite3.Connection, conversation: ConversationRow, *, include_messages: bool = True
) -> dict:
    """One conversation; with ``include_messages`` the ordered message sequence
    is embedded (the ``drift_summary_to_dict`` nested-payload precedent)."""
    payload: dict = {
        "id": conversation.id,
        "started_at": conversation.started_at,
        "updated_at": conversation.updated_at,
        "title": conversation.title,
        "message_count": count_messages(conn, conversation.id),
    }
    if include_messages:
        payload["messages"] = [
            message_to_dict(m) for m in list_messages(conn, conversation.id)
        ]
    return payload
