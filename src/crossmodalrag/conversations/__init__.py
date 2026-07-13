"""Local chat-history store (conversations + messages).

Additive and separable (the ``usage_events`` precedent): never part of any
content/derivation fingerprint and never read by retrieval/derivation. Private:
stores raw query/answer text — local-only, opt-out (``CMRAG_SAVE_HISTORY`` /
``--no-save``), clearable (``mem history --clear``).
"""

from crossmodalrag.conversations.contract import conversation_to_dict, message_to_dict
from crossmodalrag.conversations.recorder import SessionRecorder
from crossmodalrag.conversations.store import (
    ConversationRow,
    MessageRow,
    clear_conversations,
    count_messages,
    create_conversation,
    derive_title,
    get_conversation,
    list_conversations,
    list_messages,
    record_message,
    rename_conversation,
    touch_conversation,
)

__all__ = [
    "ConversationRow",
    "MessageRow",
    "SessionRecorder",
    "clear_conversations",
    "conversation_to_dict",
    "count_messages",
    "create_conversation",
    "derive_title",
    "get_conversation",
    "list_conversations",
    "list_messages",
    "message_to_dict",
    "record_message",
    "rename_conversation",
    "touch_conversation",
]
