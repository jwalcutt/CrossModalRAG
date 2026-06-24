"""Record real interaction events into the usage signal (Phase 4 step 3).

Privacy-minimal: only target id + event type + time are written — never query text.
Tracking is opt-in (see ``config.usage_tracking_enabled``) and must never break the
command that triggers it; the CLI wraps these calls defensively.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Iterable

from crossmodalrag.usage.store import record_usage_event


def record_ask_interaction(
    conn: sqlite3.Connection,
    *,
    now: datetime,
    retrieved_chunk_ids: Iterable[int] = (),
    accepted_chunk_ids: Iterable[int] = (),
    opened_node_ids: Iterable[int] = (),
) -> int:
    """Record an `ask` interaction's usage events. Returns the number of events written.

    - ``retrieved_chunk_ids`` -> ``retrieval_hit`` (chunk): the L0 evidence that was surfaced.
    - ``accepted_chunk_ids``  -> ``accepted_answer`` (chunk): evidence the user endorsed (--accept).
    - ``opened_node_ids``     -> ``open`` (node): memory nodes drilled into (ask --level).
    """
    event_at = now.isoformat()
    written = 0
    for chunk_id in dict.fromkeys(retrieved_chunk_ids):  # de-dupe, preserve order
        record_usage_event(conn, "chunk", int(chunk_id), "retrieval_hit", event_at=event_at)
        written += 1
    for chunk_id in dict.fromkeys(accepted_chunk_ids):
        record_usage_event(conn, "chunk", int(chunk_id), "accepted_answer", event_at=event_at)
        written += 1
    for node_id in dict.fromkeys(opened_node_ids):
        record_usage_event(conn, "node", int(node_id), "open", event_at=event_at)
        written += 1
    conn.commit()
    return written
