"""Best-effort persistence for one interactive chat session."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from crossmodalrag.conversations.store import (
    create_conversation,
    derive_title,
    record_message,
    touch_conversation,
)
from crossmodalrag.db import connect, init_db
from crossmodalrag.generate.answer import evidence_payload
from crossmodalrag.generate.synthesize import GeneratedAnswer


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SessionRecorder:
    """Persists one chat session's turns into ``conversations``/``messages``.

    - Lazy: no rows until the first recorded turn, so an empty session leaves
      no junk; ``new_conversation()`` (the ``/new`` command) resets to lazy.
    - Per-turn connection + one commit per turn (the ``_track_ask`` posture):
      Ctrl-C mid-session loses nothing already completed.
    - Best-effort: failures print a ``[notice]`` to stderr and NEVER raise into
      the chat loop.
    - Records LLM-path turns only, INCLUDING abstained ones (with their
      ``abstention_reason``) — unlike the in-context ``ChatSession``, history
      should show refusals. Template/no-LLM turns (``gen is None`` upstream)
      are not persisted.
    - Disabled (``enabled=False``, from ``--no-save`` / ``CMRAG_SAVE_HISTORY``)
      it is inert.
    - ``title_fn`` (optional) names a NEW conversation from its first exchange
      (e.g. an LLM title, ``naming.generate_conversation_title``); ``None`` or
      any failure falls back to the deterministic first-query title.
    - ``attach()`` continues an EXISTING conversation (resume): new turns append
      after its last turn_index and the title is left untouched.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        enabled: bool,
        now_fn: Callable[[], str] = _utc_now_iso,
        title_fn: Callable[[str, str], str | None] | None = None,
    ) -> None:
        self.db_path = db_path
        self.enabled = enabled
        self._now_fn = now_fn
        self._title_fn = title_fn
        self.conversation_id: int | None = None
        self._turn_index = 0

    def attach(self, conversation_id: int, *, next_turn_index: int) -> None:
        """Continue an existing conversation; subsequent turns append to it."""
        self.conversation_id = conversation_id
        self._turn_index = next_turn_index

    def _derive_conversation_title(self, query: str, gen: GeneratedAnswer) -> str:
        if self._title_fn is not None:
            try:
                generated = self._title_fn(query, gen.answer_text)
            except Exception:  # any naming failure falls back deterministically
                generated = None
            if generated:
                return generated
        return derive_title(query)

    def record_turn(self, query: str, gen: GeneratedAnswer) -> None:
        if not self.enabled:
            return
        try:
            now = self._now_fn()
            conn = connect(self.db_path)
            try:
                init_db(conn)
                if self.conversation_id is None:
                    self.conversation_id = create_conversation(
                        conn, started_at=now, title=self._derive_conversation_title(query, gen)
                    )
                record_message(
                    conn,
                    self.conversation_id,
                    turn_index=self._turn_index,
                    role="user",
                    text=query,
                )
                record_message(
                    conn,
                    self.conversation_id,
                    turn_index=self._turn_index,
                    role="assistant",
                    text=gen.answer_text,
                    evidence_json=json.dumps(evidence_payload(gen)),
                    abstention_reason=gen.abstention_reason,
                    truncated=gen.truncated,
                    model=gen.model,
                )
                touch_conversation(conn, self.conversation_id, updated_at=now)
                conn.commit()
                self._turn_index += 1
            finally:
                conn.close()
        except Exception as exc:  # pragma: no cover - exercised via raising-store test
            print(f"[notice] history save skipped: {exc}", file=sys.stderr)

    def new_conversation(self) -> None:
        """Start a fresh conversation (the ``/new`` command); lazy like the first."""
        self.conversation_id = None
        self._turn_index = 0
