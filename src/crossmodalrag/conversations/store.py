"""Data access for the chat-history tables (``conversations`` / ``messages``).

Write helpers do NOT commit (the recorder commits one turn atomically); the
destructive ``clear_conversations`` commits, mirroring ``clear_usage_events``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class ConversationRow:
    id: int
    started_at: str
    updated_at: str
    title: str | None


@dataclass(frozen=True)
class MessageRow:
    id: int
    conversation_id: int
    turn_index: int
    role: str
    text: str
    evidence_json: str | None
    abstention_reason: str | None
    truncated: bool
    model: str | None
    created_at: str


def derive_title(query: str, *, max_chars: int = 80) -> str:
    """A conversation title from its first user message: whitespace-collapsed, truncated."""
    collapsed = " ".join(query.split())
    if len(collapsed) <= max_chars:
        return collapsed
    return collapsed[:max_chars].rstrip() + "…"


def create_conversation(conn: sqlite3.Connection, *, started_at: str, title: str | None) -> int:
    cur = conn.execute(
        "INSERT INTO conversations (started_at, updated_at, title) VALUES (?, ?, ?)",
        (started_at, started_at, title),
    )
    return int(cur.lastrowid)


def touch_conversation(conn: sqlite3.Connection, conversation_id: int, *, updated_at: str) -> None:
    conn.execute(
        "UPDATE conversations SET updated_at = ? WHERE id = ?", (updated_at, conversation_id)
    )


def record_message(
    conn: sqlite3.Connection,
    conversation_id: int,
    *,
    turn_index: int,
    role: str,
    text: str,
    evidence_json: str | None = None,
    abstention_reason: str | None = None,
    truncated: bool = False,
    model: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO messages (conversation_id, turn_index, role, text, evidence_json, "
        "abstention_reason, truncated, model) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            conversation_id,
            turn_index,
            role,
            text,
            evidence_json,
            abstention_reason,
            1 if truncated else 0,
            model,
        ),
    )
    return int(cur.lastrowid)


def list_conversations(conn: sqlite3.Connection, *, top: int | None = None) -> list[ConversationRow]:
    """Conversations newest-first (by last activity, then id)."""
    sql = "SELECT id, started_at, updated_at, title FROM conversations ORDER BY updated_at DESC, id DESC"
    params: tuple = ()
    if top is not None:
        sql += " LIMIT ?"
        params = (top,)
    return [_row_to_conversation(row) for row in conn.execute(sql, params).fetchall()]


def get_conversation(conn: sqlite3.Connection, conversation_id: int) -> ConversationRow | None:
    row = conn.execute(
        "SELECT id, started_at, updated_at, title FROM conversations WHERE id = ?",
        (conversation_id,),
    ).fetchone()
    return _row_to_conversation(row) if row is not None else None


def list_messages(conn: sqlite3.Connection, conversation_id: int) -> list[MessageRow]:
    """A conversation's messages in insertion order."""
    rows = conn.execute(
        "SELECT id, conversation_id, turn_index, role, text, evidence_json, abstention_reason, "
        "truncated, model, created_at FROM messages WHERE conversation_id = ? ORDER BY id ASC",
        (conversation_id,),
    ).fetchall()
    return [_row_to_message(row) for row in rows]


def count_messages(conn: sqlite3.Connection, conversation_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE conversation_id = ?", (conversation_id,)
    ).fetchone()
    return int(row[0])


def clear_conversations(conn: sqlite3.Connection, *, conversation_id: int | None = None) -> int:
    """Delete saved history (all, or one conversation); returns conversations deleted.

    The user's own private data, so no extra confirmation gate (the
    ``clear_usage_events`` posture). Messages are deleted first (no DB-level
    cascade). Commits.
    """
    if conversation_id is not None:
        conn.execute("DELETE FROM messages WHERE conversation_id = ?", (conversation_id,))
        deleted = conn.execute(
            "DELETE FROM conversations WHERE id = ?", (conversation_id,)
        ).rowcount
    else:
        conn.execute("DELETE FROM messages")
        deleted = conn.execute("DELETE FROM conversations").rowcount
    conn.commit()
    return int(deleted)


def _row_to_conversation(row: sqlite3.Row) -> ConversationRow:
    return ConversationRow(
        id=int(row["id"]),
        started_at=row["started_at"],
        updated_at=row["updated_at"],
        title=row["title"],
    )


def _row_to_message(row: sqlite3.Row) -> MessageRow:
    return MessageRow(
        id=int(row["id"]),
        conversation_id=int(row["conversation_id"]),
        turn_index=int(row["turn_index"]),
        role=row["role"],
        text=row["text"],
        evidence_json=row["evidence_json"],
        abstention_reason=row["abstention_reason"],
        truncated=bool(row["truncated"]),
        model=row["model"],
        created_at=row["created_at"],
    )
