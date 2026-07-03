"""Library service layer: orchestration shared by the CLI and the local HTTP API.

Keeping the `ask` retrieval+synthesis and the `doctor` health report here (rather than inline in the
CLI) means the API is a *thin client* — it calls these functions and returns the existing JSON
contracts, adding no retrieval/derivation logic of its own.
"""

from __future__ import annotations

import sqlite3
import time

from crossmodalrag.config import (
    CONNECTOR_ENV_PREFIX,
    get_config_path,
    get_connector_paths,
    get_db_path,
    get_extract_model,
    get_llm_base_url,
    get_llm_model,
    get_llm_timeout,
    load_config,
)
from crossmodalrag.db import connect, init_db
from crossmodalrag.embed.provider import get_default_provider
from crossmodalrag.generate.answer import generated_answer_to_dict, template_answer_to_dict
from crossmodalrag.generate.provider import LLMUnavailable, get_default_llm_provider
from crossmodalrag.generate.synthesize import synthesize_answer
from crossmodalrag.memory.integrity import memory_stats
from crossmodalrag.retrieve.hybrid import DEFAULT_PROFILE, retrieve
from crossmodalrag.retrieve.nodes import candidate_chunk_ids, retrieve_nodes
from crossmodalrag.retrieve.rerank import resolve_source_types


def retrieve_for_answer(
    conn: sqlite3.Connection,
    *,
    query: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    level: str = "evidence",
    modalities: list[str] | None = None,
):
    """Retrieve evidence for a query, drilling memory-level entry points down to L0. Read-only.

    Returns ``(hits, matched_nodes)`` — the L0 evidence hits and (for non-`evidence` levels) the
    matched memory nodes that were drilled down.
    """
    restrict_source_types = resolve_source_types(modalities)
    matched_nodes = []
    if level == "evidence":
        hits = retrieve(
            conn, query=query, top_k=top_k, profile=profile, restrict_source_types=restrict_source_types
        )
    else:
        matched_nodes = retrieve_nodes(conn, query, level=level, top_k=top_k, profile=profile)
        chunk_ids = candidate_chunk_ids(conn, matched_nodes)
        hits = (
            retrieve(
                conn,
                query=query,
                top_k=top_k,
                profile=profile,
                restrict_chunk_ids=chunk_ids,
                restrict_source_types=restrict_source_types,
            )
            if chunk_ids
            else []
        )
    return hits, matched_nodes


def matched_nodes_payload(matched_nodes) -> list[dict]:
    return [
        {
            "node_id": n.node_id,
            "level": n.level,
            "node_type": n.node_type,
            "title": n.title,
            "centrality": n.centrality,
            "score": n.score,
        }
        for n in matched_nodes
    ]


def answer_payload(
    conn: sqlite3.Connection,
    *,
    query: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    level: str = "evidence",
    modalities: list[str] | None = None,
    use_llm: bool = True,
) -> dict:
    """The `mem ask --json` payload for a query (no printing, no usage tracking).

    Synthesizes a grounded answer when the LLM is available, else returns the deterministic evidence
    template. Either way the payload carries provenance for each evidence item.
    """
    start = time.monotonic()
    hits, matched_nodes = retrieve_for_answer(
        conn, query=query, top_k=top_k, profile=profile, level=level, modalities=modalities
    )
    provider = get_default_llm_provider() if use_llm else None
    if provider is not None:
        try:
            gen = synthesize_answer(query, hits, provider)
        except LLMUnavailable:
            provider = None
        else:
            data = generated_answer_to_dict(gen, total_seconds=time.monotonic() - start)
            if matched_nodes:
                data["matched_nodes"] = matched_nodes_payload(matched_nodes)
            return data

    data = template_answer_to_dict(query, hits, total_seconds=time.monotonic() - start)
    if matched_nodes:
        data["matched_nodes"] = matched_nodes_payload(matched_nodes)
    return data


def ping_ollama() -> bool:
    """Best-effort reachability check for the local Ollama server (never raises)."""
    import urllib.request

    url = f"{get_llm_base_url()}/api/tags"
    timeout = min(get_llm_timeout(), 2.0)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - localhost only
            return 200 <= resp.status < 300
    except Exception:
        return False


def health_report() -> dict:
    """Read-only health report: DB, installed extras, Ollama reachability, models, config, memory.

    The payload behind `mem doctor` and the API `/health` endpoint.
    """
    from crossmodalrag.capabilities import has_ocr, has_pdf

    db_path = get_db_path()
    db_exists = db_path.exists()
    provider = get_default_provider()
    embed_model = provider.name if provider is not None else None

    stats = None
    if db_exists:
        conn = connect(db_path)
        try:
            init_db(conn)
            stats = memory_stats(conn)
        finally:
            conn.close()

    config_path = get_config_path()
    return {
        "db": {
            "path": str(db_path),
            "exists": db_exists,
            "size_bytes": (db_path.stat().st_size if db_exists else 0),
        },
        "extras": {"embeddings": provider is not None, "pdf": has_pdf(), "ocr": has_ocr()},
        "ollama": {"base_url": get_llm_base_url(), "reachable": ping_ollama()},
        "models": {"embed": embed_model, "llm": get_llm_model(), "extract": get_extract_model()},
        "config": {"path": (str(config_path) if config_path is not None else None), "loaded": bool(load_config())},
        "connectors": {name: len(get_connector_paths(name)) for name in CONNECTOR_ENV_PREFIX},
        "memory": stats,
    }
