from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from crossmodalrag.config import (
    get_db_path,
    get_extract_model,
    get_numbered_env_paths,
    load_dotenv,
    usage_tracking_enabled,
)
from crossmodalrag.db import connect, init_db
from crossmodalrag.embed.provider import (
    DEFAULT_EMBED_MODEL,
    MissingEmbeddingBackend,
    get_default_provider,
    require_default_provider,
)
from crossmodalrag.embed.provider import get_default_provider
from crossmodalrag.embed.store import (
    count_embeddings,
    count_node_embeddings,
    embed_pending_chunks,
    embed_pending_nodes,
)
from crossmodalrag.evaluation import load_eval_queries_file, run_eval, upsert_eval_queries
from crossmodalrag.generate.answer import (
    format_generated_answer,
    format_grounded_answer,
    generated_answer_to_dict,
)
from crossmodalrag.generate.provider import LLMUnavailable, get_default_llm_provider
from crossmodalrag.generate.synthesize import synthesize_answer
from crossmodalrag.generation_eval import run_generation_eval
from crossmodalrag.capabilities import MissingModalityBackend
from crossmodalrag.ingest.git import ingest_git
from crossmodalrag.ingest.image import ingest_images
from crossmodalrag.ingest.notes import ingest_notes
from crossmodalrag.ingest.pdf import ingest_pdf
from crossmodalrag.modality import format_locator, parse_locator
from crossmodalrag.retrieve.rerank import MODALITY_SOURCE_TYPES, resolve_source_types
from crossmodalrag.memory.concepts import build_concepts
from crossmodalrag.memory.episodes import build_episodes
from crossmodalrag.memory.extract import extract_pending_sources
from crossmodalrag.memory.graph import build_graph
from crossmodalrag.memory.integrity import (
    count_edges,
    count_nodes_by_level,
    count_nodes_by_type,
    find_dangling_edges,
    find_unsupported_nodes,
)
from crossmodalrag.retrieve.hybrid import DEFAULT_PROFILE, PROFILE_WEIGHTS, retrieve
from crossmodalrag.retrieve.nodes import candidate_chunk_ids, retrieve_nodes
from crossmodalrag.sample_data import default_sample_db_path, seed_sample_data

def init_db_cmd() -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
    finally:
        conn.close()
    print(f"Initialized database at {db_path}")


def ingest_notes_cmd(vault_paths: list[Path]) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    embedder = get_default_provider()
    try:
        init_db(conn)
        total_inserted = 0
        for vault_path in vault_paths:
            inserted = ingest_notes(conn, vault_path=vault_path, embedder=embedder)
            total_inserted += inserted
            print(f"Ingested notes from {vault_path} into {db_path}. Inserted chunks: {inserted}")
    finally:
        conn.close()
    print(
        f"Completed note ingestion for {len(vault_paths)} vault(s) into {db_path}. "
        f"Total inserted chunks: {total_inserted}"
    )


def ingest_git_cmd(repo_paths: list[Path], max_commits: int = 300) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    embedder = get_default_provider()
    try:
        init_db(conn)
        total_inserted = 0
        for repo_path in repo_paths:
            inserted = ingest_git(
                conn, repo_path=repo_path, max_commits=max_commits, embedder=embedder
            )
            total_inserted += inserted
            print(
                f"Ingested git history from {repo_path} into {db_path}. Inserted chunks: {inserted}"
            )
    finally:
        conn.close()
    print(
        f"Completed git ingestion for {len(repo_paths)} repo(s) into {db_path}. "
        f"Total inserted chunks: {total_inserted}"
    )


def ingest_pdf_cmd(pdf_paths: list[Path]) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    embedder = get_default_provider()
    try:
        init_db(conn)
        total_inserted = 0
        for pdf_path in pdf_paths:
            inserted = ingest_pdf(conn, pdf_path=pdf_path, embedder=embedder)
            total_inserted += inserted
            print(f"Ingested PDF(s) from {pdf_path} into {db_path}. Inserted chunks: {inserted}")
    except MissingModalityBackend as exc:
        raise SystemExit(str(exc))
    finally:
        conn.close()
    print(
        f"Completed PDF ingestion for {len(pdf_paths)} path(s) into {db_path}. "
        f"Total inserted chunks: {total_inserted}"
    )


def ingest_images_cmd(image_paths: list[Path]) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    embedder = get_default_provider()
    try:
        init_db(conn)
        total_inserted = 0
        for image_path in image_paths:
            inserted = ingest_images(conn, image_path=image_path, embedder=embedder)
            total_inserted += inserted
            print(f"Ingested image(s) from {image_path} into {db_path}. Inserted chunks: {inserted}")
    except MissingModalityBackend as exc:
        raise SystemExit(str(exc))
    finally:
        conn.close()
    print(
        f"Completed image ingestion for {len(image_paths)} path(s) into {db_path}. "
        f"Total inserted chunks: {total_inserted}"
    )


def ask_cmd(
    query: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    explain: bool = False,
    use_llm: bool = True,
    as_json: bool = False,
    debug: bool = False,
    level: str = "evidence",
    modalities: list[str] | None = None,
    accept: bool = False,
    track: bool | None = None,
) -> None:
    db_path = get_db_path()
    restrict_source_types = resolve_source_types(modalities)
    # Usage tracking is opt-in: off unless enabled by env / --track / --accept, and never by --no-track.
    track_enabled = False if track is False else (bool(track) or accept or usage_tracking_enabled())
    conn = connect(db_path)
    matched_nodes = []
    try:
        if level == "evidence":
            hits = retrieve(
                conn,
                query=query,
                top_k=top_k,
                profile=profile,
                restrict_source_types=restrict_source_types,
            )
        else:
            # Retrieve at the target level, then ground in the matched nodes' L0 evidence.
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
    finally:
        conn.close()

    matched_payload = [
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

    provider = get_default_llm_provider() if use_llm else None
    if provider is not None:
        try:
            gen = synthesize_answer(query, hits, provider)
        except LLMUnavailable as exc:
            print(f"[notice] LLM unavailable, falling back to evidence template: {exc}", file=sys.stderr)
            provider = None
        else:
            if as_json:
                data = generated_answer_to_dict(gen)
                if matched_payload:
                    data["matched_nodes"] = matched_payload
                print(json.dumps(data, indent=2))
            else:
                _print_matched_nodes(matched_payload, level)
                print(format_generated_answer(gen, explain=explain, debug=debug))
            if track_enabled and not gen.abstained:
                cited = [gen.id_map[eid].chunk_id for eid in gen.cited_evidence_ids if eid in gen.id_map]
                _track_ask(
                    db_path,
                    hits=hits,
                    matched_nodes=matched_nodes,
                    accepted_chunk_ids=(cited or [h.chunk_id for h in hits]) if accept else [],
                )
            return

    # No LLM (disabled or unavailable): deterministic evidence template.
    if as_json:
        data = {
            "query": query,
            "model": None,
            "abstained": not hits,
            "answer": None,
            "evidence": [
                {
                    "evidence_id": f"E{i}",
                    "source_id": hit.source_id,
                    "chunk_id": hit.chunk_id,
                    "source_type": hit.source_type,
                    "source_uri": hit.source_uri,
                    "title": hit.title,
                    "modality": (loc.modality if (loc := parse_locator(hit.chunk_metadata_json)) else None),
                    "locator": format_locator(hit.source_uri, parse_locator(hit.chunk_metadata_json)),
                    "page": (loc.page if (loc := parse_locator(hit.chunk_metadata_json)) else None),
                    "ocr_confidence": (
                        loc.ocr_confidence if (loc := parse_locator(hit.chunk_metadata_json)) else None
                    ),
                    "scores": {
                        "combined": hit.score,
                        "vector": hit.vector_score,
                        "lexical": hit.lexical_score,
                        "recency": hit.recency_score,
                        "usage": hit.usage_score,
                    },
                }
                for i, hit in enumerate(hits, start=1)
            ],
        }
        if matched_payload:
            data["matched_nodes"] = matched_payload
        print(json.dumps(data, indent=2))
    else:
        _print_matched_nodes(matched_payload, level)
        print(format_grounded_answer(query, hits, explain=explain or debug))

    # No-LLM / fallback path: the template shows `hits` directly; no citations to scope `--accept`.
    if track_enabled and hits:
        _track_ask(
            db_path,
            hits=hits,
            matched_nodes=matched_nodes,
            accepted_chunk_ids=[h.chunk_id for h in hits] if accept else [],
        )


def _track_ask(db_path, *, hits, matched_nodes, accepted_chunk_ids) -> None:
    """Best-effort usage tracking for an `ask` (never raises into the command path)."""
    from datetime import datetime, timezone

    from crossmodalrag.usage.tracking import record_ask_interaction

    try:
        conn = connect(db_path)
        try:
            init_db(conn)
            record_ask_interaction(
                conn,
                now=datetime.now(timezone.utc),
                retrieved_chunk_ids=[h.chunk_id for h in hits],
                accepted_chunk_ids=accepted_chunk_ids,
                opened_node_ids=[n.node_id for n in matched_nodes],
            )
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - tracking must never break a query
        print(f"[notice] usage tracking skipped: {exc}", file=sys.stderr)


def _print_matched_nodes(matched_payload: list[dict], level: str) -> None:
    if not matched_payload:
        return
    print(f"Matched {level}s:")
    for node in matched_payload:
        title = node["title"] or "untitled"
        print(f"  #{node['node_id']} (score={node['score']:.3f}, centrality={node['centrality']:.3f}): {title}")
    print()


def eval_cmd(
    top_k: int = 5,
    query_prefix: str | None = None,
    load_queries_path: Path | None = None,
    profile: str = DEFAULT_PROFILE,
    level: str = "evidence",
    modalities: list[str] | None = None,
) -> None:
    db_path = get_db_path()
    restrict_source_types = resolve_source_types(modalities)
    conn = connect(db_path)
    try:
        init_db(conn)
        loaded = 0
        if load_queries_path is not None:
            queries = load_eval_queries_file(load_queries_path)
            loaded = upsert_eval_queries(conn, queries)
        summary = run_eval(
            conn,
            top_k=top_k,
            query_prefix=query_prefix,
            profile=profile,
            level=level,
            restrict_source_types=restrict_source_types,
        )
    finally:
        conn.close()

    print(f"Evaluation DB: {db_path}")
    print(f"Retrieval level: {level} | profile: {profile}")
    if load_queries_path is not None:
        print(f"Eval queries loaded/upserted: {loaded} from {load_queries_path}")
    if query_prefix:
        print(f"Query prefix filter: {query_prefix}")
    if summary.query_count == 0:
        print("No evaluation queries found. Load queries into 'queries_eval' and run again.")
        return
    print(f"Queries evaluated: {summary.query_count}")
    print(f"Recall@{summary.top_k}: {summary.recall_at_k:.3f}")
    print(f"MRR@{summary.top_k}: {summary.mrr_at_k:.3f}")
    print(f"Citation hit-rate (top-1): {summary.citation_hit_rate:.3f}")
    misses = [r.query_text for r in summary.results if r.first_correct_rank is None]
    if misses:
        print(f"Queries with no correct hit in top-{summary.top_k} ({len(misses)}):")
        for query_text in misses:
            print(f"  - {query_text}")


def reindex_embeddings_cmd(batch_size: int = 64, model: str | None = None) -> None:
    db_path = get_db_path()
    try:
        provider = require_default_provider(model)
    except MissingEmbeddingBackend as exc:
        raise SystemExit(str(exc))
    conn = connect(db_path)
    try:
        init_db(conn)
        embedded = embed_pending_chunks(conn, provider, batch_size=batch_size)
        total = count_embeddings(conn, model=provider.name)
        nodes_embedded = 0
        for node_type, lvl in (("event", 1), ("episode", 2), ("concept", 3)):
            nodes_embedded += embed_pending_nodes(
                conn, provider, level=lvl, node_type=node_type, batch_size=batch_size
            )
        node_total = count_node_embeddings(conn, model=provider.name)
    finally:
        conn.close()
    print(f"Reindex DB: {db_path}")
    print(f"Model: {provider.name} (dim={provider.dim})")
    print(f"Chunks embedded this run: {embedded}")
    print(f"Total chunks with current-model embeddings: {total}")
    print(f"Memory nodes embedded this run: {nodes_embedded}")
    print(f"Total nodes with current-model embeddings: {node_total}")


def eval_generation_cmd(
    top_k: int = 5,
    query_prefix: str | None = None,
    profile: str = DEFAULT_PROFILE,
    model: str | None = None,
    level: str = "evidence",
) -> None:
    db_path = get_db_path()
    provider = get_default_llm_provider()
    if provider is None:
        raise SystemExit("No LLM provider configured (set CMRAG_LLM_PROVIDER).")
    if model:
        provider.name = model
    conn = connect(db_path)
    try:
        init_db(conn)
        try:
            summary = run_generation_eval(
                conn, provider, top_k=top_k, query_prefix=query_prefix, profile=profile, level=level
            )
        except LLMUnavailable as exc:
            raise SystemExit(str(exc))
    finally:
        conn.close()

    print(f"Evaluation DB: {db_path}")
    print(f"Model: {summary.model} | level: {summary.level} | profile: {summary.profile}")
    if query_prefix:
        print(f"Query prefix filter: {query_prefix}")
    if summary.query_count == 0:
        print("No evaluation queries found. Load queries into 'queries_eval' and run again.")
        return
    print(f"Queries evaluated: {summary.query_count}")
    print(f"Citation validity: {summary.citation_validity:.3f}")
    print(f"Source-grounding hit: {summary.source_grounding_hit:.3f}")
    print(f"Source coverage: {summary.source_coverage:.3f}")
    print(f"Abstention correct: {summary.abstention_correct:.3f}")
    bad = [r.query_text for r in summary.results if not r.abstention_correct or not r.citation_valid]
    if bad:
        print(f"Queries with citation/abstention issues ({len(bad)}):")
        for query_text in bad:
            print(f"  - {query_text}")


def build_memory_cmd(level: str = "all", limit: int | None = None, model: str | None = None) -> None:
    build_events = level in ("event", "all")
    build_eps = level in ("episode", "all")
    build_concept = level in ("concept", "all")
    build_graph_layer = level in ("graph", "all")
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        print(f"Memory DB: {db_path}")

        if build_events:
            provider = get_default_llm_provider(model or get_extract_model())
            if provider is None:
                raise SystemExit("No LLM provider configured (set CMRAG_LLM_PROVIDER).")
            try:
                result = extract_pending_sources(conn, provider, limit=limit)
            except LLMUnavailable as exc:
                raise SystemExit(str(exc))
            print(f"L1 events | model: {provider.name}")
            print(f"  Sources processed: {result.sources_processed}")
            print(f"  Sources skipped (up to date): {result.sources_skipped}")
            print(f"  Events created: {result.events_created}")
            if result.parse_failures:
                print(f"  Unparseable sources (will retry next run): {result.parse_failures}")

        if build_eps:
            episodes = build_episodes(conn)
            print("L2 episodes")
            print(f"  Episodes created: {episodes.episodes_created}")
            print(f"  Episodes kept (up to date): {episodes.episodes_kept}")
            print(f"  Episodes deleted (stale): {episodes.episodes_deleted}")
            print(f"  Events grouped: {episodes.events_grouped}")

        if build_concept:
            embed_provider = get_default_provider()
            if embed_provider is None:
                raise SystemExit(
                    "L3 concepts require the embeddings extra. Run: pip install -e \".[embeddings]\""
                )
            llm_provider = get_default_llm_provider(model or get_extract_model())
            concepts = build_concepts(conn, embed_provider, llm_provider)
            print(f"L3 concepts | embed: {embed_provider.name}")
            print(f"  Concepts created: {concepts.concepts_created}")
            print(f"  Concepts kept (up to date): {concepts.concepts_kept}")
            print(f"  Concepts deleted (stale): {concepts.concepts_deleted}")
            print(f"  Events clustered: {concepts.events_clustered} (unclustered: {concepts.events_unclustered})")
            if concepts.concepts_created:
                print(f"  Named by LLM: {concepts.named_by_llm} | by fallback: {concepts.named_by_fallback}")

        if build_graph_layer:
            graph = build_graph(conn)
            print("Graph")
            print(f"  Concept co-occurrence edges: {graph.relates_edges_created} "
                  f"(replaced {graph.relates_edges_deleted})")
            print(f"  Nodes scored (centrality): {graph.nodes_scored}")
    finally:
        conn.close()
    print("Run `mem memory-stats` to inspect node/edge counts and integrity.")


def memory_stats_cmd() -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        by_level = count_nodes_by_level(conn)
        by_type = count_nodes_by_type(conn)
        edges = count_edges(conn)
        unsupported = find_unsupported_nodes(conn)
        dangling = find_dangling_edges(conn)
        node_vectors = count_node_embeddings(conn)
        relates_edges = conn.execute(
            "SELECT COUNT(*) AS n FROM memory_edges WHERE relation = 'relates_to'"
        ).fetchone()["n"]
        top_central = conn.execute(
            "SELECT id, level, title, centrality FROM memory_nodes "
            "WHERE centrality IS NOT NULL ORDER BY centrality DESC, id ASC LIMIT 3"
        ).fetchall()
    finally:
        conn.close()

    total_nodes = sum(by_level.values())
    print(f"Memory DB: {db_path}")
    print(f"Memory nodes (L1-L3): {total_nodes}")
    for level in (1, 2, 3):
        print(f"  L{level}: {by_level.get(level, 0)}")
    if by_type:
        print("By type: " + ", ".join(f"{name}={count}" for name, count in by_type.items()))
    print(f"Memory edges: {edges}")
    print(f"Concept co-occurrence edges (relates_to): {relates_edges}")
    print(f"Node embeddings: {node_vectors}")
    if top_central:
        print("Top central nodes:")
        for row in top_central:
            title = row["title"] or "untitled"
            print(f"  L{row['level']} #{row['id']} ({row['centrality']:.3f}): {title}")
    print("Integrity:")
    print(f"  unsupported nodes (no L0 evidence): {len(unsupported)}")
    print(f"  dangling edges (missing endpoint): {len(dangling)}")
    if unsupported:
        print(f"  unsupported node ids: {unsupported}")
    if dangling:
        print(f"  dangling edge ids: {dangling}")


def usage_cmd(clear: bool = False, top: int = 10) -> None:
    from datetime import datetime, timezone

    from crossmodalrag.config import get_usage_halflife_days, usage_tracking_enabled
    from crossmodalrag.usage.store import clear_usage_events, usage_summaries

    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        if clear:
            deleted = clear_usage_events(conn)
            print(f"Cleared {deleted} usage event(s) from {db_path}.")
            return

        total = conn.execute("SELECT COUNT(*) AS n FROM usage_events").fetchone()["n"]
        by_type = conn.execute(
            "SELECT event_type, COUNT(*) AS n FROM usage_events GROUP BY event_type ORDER BY event_type"
        ).fetchall()
        summaries = usage_summaries(
            conn, now=datetime.now(timezone.utc), halflife_days=get_usage_halflife_days()
        )
    finally:
        conn.close()

    print(f"Usage DB: {db_path}")
    print(f"Tracking enabled (env): {usage_tracking_enabled()}")
    print(f"Total usage events: {total}")
    if by_type:
        print("By type: " + ", ".join(f"{r['event_type']}={r['n']}" for r in by_type))
    top_targets = sorted(summaries.values(), key=lambda s: s.strength, reverse=True)[:top]
    if top_targets:
        print(f"Top {len(top_targets)} targets by rehearsal strength:")
        for s in top_targets:
            print(f"  {s.target_kind} #{s.target_id}: strength={s.strength:.3f} "
                  f"events={s.count} last={s.last_event_at}")


def forgetting_cmd(level: str = "concept", top: int = 10, min_support: int = 1) -> None:
    from datetime import datetime, timezone

    from crossmodalrag.config import get_usage_halflife_days
    from crossmodalrag.memory.forgetting import LEVEL_NAMES, compute_forgetting_risk

    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        items = compute_forgetting_risk(
            conn,
            now=datetime.now(timezone.utc),
            halflife_days=get_usage_halflife_days(),
            levels=LEVEL_NAMES[level],
            min_support=min_support,
            top=top,
        )
    finally:
        conn.close()

    print(f"Forgetting risk (level={level}) — DB: {db_path}")
    if not items:
        print(
            f"No {level} memory nodes with grounding found. Run `mem build-memory` first, "
            "or try `--level all`."
        )
        return
    print("What you're most likely forgetting (important but not recently revisited):")
    for item in items:
        title = item.title or "untitled"
        print(
            f"  [risk={item.risk:.3f}] L{item.level} {item.node_type}: {title}\n"
            f"      importance={item.importance:.3f} staleness={item.staleness:.3f} "
            f"confidence={item.confidence:.3f} (support={item.support}, last_touch={item.last_touch})"
        )
        if item.evidence_source_uris:
            print(f"      evidence: {', '.join(item.evidence_source_uris)}")


def recall_cmd(level: str = "concept", top: int = 10, min_support: int = 1, regenerate: bool = False) -> None:
    from datetime import datetime, timezone

    from crossmodalrag.config import get_usage_halflife_days
    from crossmodalrag.memory.forgetting import LEVEL_NAMES
    from crossmodalrag.memory.recall import generate_recall_cards

    db_path = get_db_path()
    provider = get_default_llm_provider(get_extract_model())
    conn = connect(db_path)
    try:
        init_db(conn)
        cards = generate_recall_cards(
            conn,
            provider,
            now=datetime.now(timezone.utc),
            halflife_days=get_usage_halflife_days(),
            levels=LEVEL_NAMES[level],
            top=top,
            min_support=min_support,
            regenerate=regenerate,
        )
    finally:
        conn.close()

    print(f"Active-recall cards (level={level}) — DB: {db_path}")
    if not cards:
        print(
            f"No {level} memory nodes with grounding found. Run `mem build-memory` first, "
            "or try `--level all`."
        )
        return
    print("Quiz yourself on what you're most likely forgetting:")
    for card in cards:
        title = card.title or "untitled"
        print(f"  [risk={card.risk:.3f} | {card.generated_by}] L{card.level} {card.node_type}: {title}")
        print(f"      Q: {card.question}")
        if card.answer:
            print(f"      A: {card.answer}")
        if card.evidence_source_uris:
            print(f"      evidence: {', '.join(card.evidence_source_uris)}")


def concepts_cmd(top: int = 20) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT n.id AS id, n.title AS title, n.centrality AS centrality,
                   COUNT(e.id) AS members
            FROM memory_nodes n
            LEFT JOIN memory_edges e
                ON e.parent_level = 3 AND e.parent_id = n.id AND e.relation = 'contains'
            WHERE n.level = 3 AND n.node_type = 'concept'
            GROUP BY n.id
            ORDER BY n.centrality DESC NULLS LAST, n.id ASC
            LIMIT ?
            """,
            (top,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        print("No concepts yet. Run `mem build-memory` (needs the embeddings extra).")
        return
    print(f"Concepts (top {len(rows)} by centrality):")
    for row in rows:
        cen = row["centrality"] if row["centrality"] is not None else 0.0
        print(f"  #{row['id']} (centrality={cen:.3f}, {row['members']} events): {row['title'] or 'untitled'}")


def timeline_cmd(limit: int = 50) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT n.id AS id, n.title AS title, n.time_start AS time_start, n.time_end AS time_end,
                   COUNT(e.id) AS members
            FROM memory_nodes n
            LEFT JOIN memory_edges e
                ON e.parent_level = 2 AND e.parent_id = n.id AND e.relation = 'contains'
            WHERE n.level = 2 AND n.node_type = 'episode'
            GROUP BY n.id
            ORDER BY n.time_start ASC NULLS LAST, n.id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        print("No episodes yet. Run `mem build-memory`.")
        return
    print(f"Timeline ({len(rows)} episodes, oldest first):")
    for row in rows:
        start = (row["time_start"] or "?")[:10]
        end = (row["time_end"] or "?")[:10]
        span = start if start == end else f"{start}..{end}"
        print(f"  #{row['id']} [{span}] ({row['members']} events): {row['title'] or 'untitled'}")


def seed_sample_cmd(
    workspace_dir: Path,
    force: bool = False,
    db_path: Path | None = None,
) -> None:
    db_path = (db_path or default_sample_db_path()).expanduser().resolve()
    conn = connect(db_path)
    try:
        init_db(conn)
        result = seed_sample_data(conn, workspace_dir=workspace_dir, force=force)
    finally:
        conn.close()
    print(f"Seeded sample data into sample DB: {db_path}")
    print("Main DB was not modified.")
    print(f"Workspace: {result.workspace_dir}")
    print(f"Sample vault: {result.vault_dir}")
    print(f"Sample git repo: {result.repo_dir}")
    print(
        "Inserted chunks "
        f"(notes={result.notes_chunks_inserted}, git={result.git_chunks_inserted}, "
        f"pdf={result.pdf_chunks_inserted}, image={result.image_chunks_inserted}); "
        f"eval queries upserted={result.eval_queries_upserted}; "
        f"usage events seeded={result.usage_events_seeded}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CrossModalRAG local memory CLI.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize SQLite database schema.")

    p_notes = sub.add_parser(
        "ingest-notes",
        help="Ingest markdown notes from one or more vault paths (or use OBSIDIAN_VAULT_PATH_* from .env).",
    )
    p_notes.add_argument("vault_paths", nargs="*", type=Path)

    p_git = sub.add_parser(
        "ingest-git",
        help="Ingest git commits and diffs from one or more repos (or use REPO_PATH_* from .env).",
    )
    p_git.add_argument("repo_paths", nargs="*", type=Path)
    p_git.add_argument("--max-commits", type=int, default=300)

    p_pdf = sub.add_parser(
        "ingest-pdf",
        help="Ingest PDF text (per-page) from one or more files/dirs (or use PDF_PATH_* from .env). "
        'Requires the [pdf] extra: pip install -e ".[pdf]".',
    )
    p_pdf.add_argument("pdf_paths", nargs="*", type=Path)

    p_img = sub.add_parser(
        "ingest-images",
        help="Ingest image OCR text from one or more files/dirs (or use IMAGE_PATH_* from .env). "
        'Requires the [ocr] extra (pip install -e ".[ocr]") and a local tesseract binary.',
    )
    p_img.add_argument("image_paths", nargs="*", type=Path)

    profile_choices = sorted(PROFILE_WEIGHTS)

    level_choices = ["evidence", "event", "episode", "concept"]
    modality_choices = sorted(MODALITY_SOURCE_TYPES)

    p_ask = sub.add_parser("ask", help="Query indexed evidence.")
    p_ask.add_argument("query", type=str)
    p_ask.add_argument("--top-k", type=int, default=5)
    p_ask.add_argument(
        "--level",
        choices=level_choices,
        default="evidence",
        help="Retrieval level: 'evidence' (L0 chunks, default) or a memory level "
        "(event/episode/concept), which drills matched nodes down to L0 for grounding.",
    )
    p_ask.add_argument(
        "--profile",
        choices=profile_choices,
        default=DEFAULT_PROFILE,
        help="Hybrid retrieval profile (vector/lexical/recency blend).",
    )
    p_ask.add_argument(
        "--explain",
        action="store_true",
        help="Show per-hit score components (vector/lexical/recency/combined).",
    )
    p_ask.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip LLM synthesis; return the deterministic evidence template.",
    )
    p_ask.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Emit a structured JSON answer (stable contract for UIs).",
    )
    p_ask.add_argument(
        "--debug",
        action="store_true",
        help="Include retrieval diagnostics, the raw prompt, and raw model output.",
    )
    p_ask.add_argument(
        "--modality",
        choices=modality_choices,
        action="append",
        default=None,
        help="Restrict evidence to one or more modalities (repeatable). "
        "Maps to source types: text=notes, code=git, pdf=PDFs, image=OCR'd images.",
    )
    p_ask.add_argument(
        "--accept",
        action="store_true",
        help="Record this answer as accepted (usage feedback on the cited evidence); enables tracking.",
    )
    p_ask.add_argument(
        "--track",
        action="store_true",
        help="Log usage events for this query (overrides CMRAG_USAGE_TRACKING for the call).",
    )
    p_ask.add_argument(
        "--no-track",
        action="store_true",
        help="Do not log usage events for this query, even if tracking is enabled by env.",
    )

    p_eval = sub.add_parser(
        "eval",
        help="Run retrieval evaluation using queries stored in queries_eval (optionally load from JSON).",
    )
    p_eval.add_argument("--top-k", type=int, default=5)
    p_eval.add_argument(
        "--query-prefix",
        type=str,
        default=None,
        help="Only evaluate queries whose text starts with this prefix (e.g. '[sample]').",
    )
    p_eval.add_argument(
        "--load-queries",
        type=Path,
        default=None,
        help="JSON file of eval query rows to upsert before running evaluation.",
    )
    p_eval.add_argument(
        "--profile",
        choices=profile_choices,
        default=DEFAULT_PROFILE,
        help="Hybrid retrieval profile (vector/lexical/recency blend).",
    )
    p_eval.add_argument(
        "--level",
        choices=level_choices,
        default="evidence",
        help="Retrieval level: 'evidence' (default) or a memory level evaluated via drill-down.",
    )
    p_eval.add_argument(
        "--modality",
        choices=modality_choices,
        action="append",
        default=None,
        help="Restrict evidence to one or more modalities (repeatable): text/code/pdf/image.",
    )

    p_reindex = sub.add_parser(
        "reindex-embeddings",
        help="Backfill/repair chunk embeddings for the active model (requires the 'embeddings' extra).",
    )
    p_reindex.add_argument("--batch-size", type=int, default=64)
    p_reindex.add_argument(
        "--model",
        type=str,
        default=None,
        help=f"Embedding model id (defaults to CMRAG_EMBED_MODEL or {DEFAULT_EMBED_MODEL}).",
    )

    p_eval_gen = sub.add_parser(
        "eval-generation",
        help="Evaluate grounded answer synthesis (citation validity, source grounding, abstention).",
    )
    p_eval_gen.add_argument("--top-k", type=int, default=5)
    p_eval_gen.add_argument(
        "--query-prefix",
        type=str,
        default=None,
        help="Only evaluate queries whose text starts with this prefix (e.g. '[sample]').",
    )
    p_eval_gen.add_argument(
        "--profile",
        choices=profile_choices,
        default=DEFAULT_PROFILE,
        help="Hybrid retrieval profile (vector/lexical/recency blend).",
    )
    p_eval_gen.add_argument(
        "--level",
        choices=level_choices,
        default="evidence",
        help="Retrieval level: 'evidence' (default) or a memory level; higher levels drill "
        "matched nodes down to L0 before synthesis (answers still cite L0).",
    )
    p_eval_gen.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model id (defaults to CMRAG_LLM_MODEL).",
    )

    p_build = sub.add_parser(
        "build-memory",
        help="Derive hierarchical memory from L0 evidence (L1 events via Ollama; L2 episodes deterministic).",
    )
    p_build.add_argument(
        "--level",
        choices=["event", "episode", "concept", "graph", "all"],
        default="all",
        help="Memory level to build: 'event' (LLM), 'episode' (no LLM), "
        "'concept' (embeddings extra; LLM naming optional), 'graph' (no LLM/embeddings), "
        "or 'all' (default).",
    )
    p_build.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of sources to (re)process for event extraction this run (resumable).",
    )
    p_build.add_argument(
        "--model",
        type=str,
        default=None,
        help="Extraction model id (defaults to CMRAG_EXTRACT_MODEL).",
    )

    sub.add_parser(
        "memory-stats",
        help="Show hierarchical memory node/edge counts and structural integrity status.",
    )

    p_concepts = sub.add_parser("concepts", help="List L3 concepts ranked by centrality.")
    p_concepts.add_argument("--top", type=int, default=20)

    p_timeline = sub.add_parser("timeline", help="List L2 episodes chronologically.")
    p_timeline.add_argument("--limit", type=int, default=50)

    p_usage = sub.add_parser(
        "usage",
        help="Show local usage-tracking stats (read-only), or --clear to wipe usage history.",
    )
    p_usage.add_argument("--clear", action="store_true", help="Delete all usage events (local).")
    p_usage.add_argument("--top", type=int, default=10, help="How many top targets to show.")

    p_forgetting = sub.add_parser(
        "forgetting",
        help="Rank important-but-stale memories ('what am I likely forgetting?'). Read-only.",
    )
    p_forgetting.add_argument(
        "--level",
        choices=["concept", "episode", "event", "all"],
        default="concept",
        help="Which memory level to score (default concept).",
    )
    p_forgetting.add_argument("--top", type=int, default=10)
    p_forgetting.add_argument(
        "--min-support",
        type=int,
        default=1,
        help="Minimum grounded L0 chunks for a node to be considered.",
    )

    p_recall = sub.add_parser(
        "recall",
        help="Generate grounded active-recall study cards for the memories you're most likely "
        "forgetting (local LLM, cached; deterministic fallback when Ollama is absent).",
    )
    p_recall.add_argument(
        "--level",
        choices=["concept", "episode", "event", "all"],
        default="concept",
        help="Which memory level to generate cards for (default concept).",
    )
    p_recall.add_argument("--top", type=int, default=10)
    p_recall.add_argument(
        "--min-support",
        type=int,
        default=1,
        help="Minimum grounded L0 chunks for a node to be considered.",
    )
    p_recall.add_argument(
        "--regenerate",
        action="store_true",
        help="Force regeneration of cards (e.g. to upgrade fallback cards once Ollama is available).",
    )

    p_seed = sub.add_parser(
        "seed-sample",
        help="Create deterministic synthetic notes/git fixtures and ingest them into the DB.",
    )
    p_seed.add_argument(
        "--workspace-dir",
        type=Path,
        default=Path("data") / "sample-seed-workspace",
        help="Directory for generated synthetic sample vault and git repo.",
    )
    p_seed.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="DB path for synthetic sample data (defaults to a temp DB, not the main memory DB).",
    )
    p_seed.add_argument(
        "--force",
        action="store_true",
        help="Rebuild the sample workspace directory if it already exists (destructive).",
    )

    return parser


def main() -> None:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init-db":
        init_db_cmd()
        return
    if args.command == "ingest-notes":
        vault_paths = _resolve_ingest_paths(
            args.vault_paths,
            env_prefix="OBSIDIAN_VAULT_PATH",
            command_name="ingest-notes",
        )
        if not vault_paths:
            parser.error(
                "No vault paths provided. Use `mem ingest-notes <vault_path> [<vault_path> ...]` "
                "or define `OBSIDIAN_VAULT_PATH_1`, `OBSIDIAN_VAULT_PATH_2`, ... in your local .env "
                "for default ingestion targets."
            )
        ingest_notes_cmd(vault_paths)
        return
    if args.command == "ingest-git":
        repo_paths = _resolve_ingest_paths(
            args.repo_paths,
            env_prefix="REPO_PATH",
            command_name="ingest-git",
        )
        if not repo_paths:
            parser.error(
                "No repo paths provided. Use `mem ingest-git <repo_path> [<repo_path> ...] [--max-commits N]` "
                "or define `REPO_PATH_1`, `REPO_PATH_2`, ... in your local .env "
                "for default ingestion targets."
            )
        ingest_git_cmd(repo_paths, max_commits=args.max_commits)
        return
    if args.command == "ingest-pdf":
        pdf_paths = _resolve_ingest_paths(
            args.pdf_paths,
            env_prefix="PDF_PATH",
            command_name="ingest-pdf",
        )
        if not pdf_paths:
            parser.error(
                "No PDF paths provided. Use `mem ingest-pdf <path> [<path> ...]` "
                "or define `PDF_PATH_1`, `PDF_PATH_2`, ... in your local .env "
                "for default ingestion targets."
            )
        ingest_pdf_cmd(pdf_paths)
        return
    if args.command == "ingest-images":
        image_paths = _resolve_ingest_paths(
            args.image_paths,
            env_prefix="IMAGE_PATH",
            command_name="ingest-images",
        )
        if not image_paths:
            parser.error(
                "No image paths provided. Use `mem ingest-images <path> [<path> ...]` "
                "or define `IMAGE_PATH_1`, `IMAGE_PATH_2`, ... in your local .env "
                "for default ingestion targets."
            )
        ingest_images_cmd(image_paths)
        return
    if args.command == "ask":
        track = True if args.track else (False if args.no_track else None)
        ask_cmd(
            args.query,
            top_k=args.top_k,
            profile=args.profile,
            explain=args.explain,
            use_llm=not args.no_llm,
            as_json=args.as_json,
            debug=args.debug,
            level=args.level,
            modalities=args.modality,
            accept=args.accept,
            track=track,
        )
        return
    if args.command == "eval":
        eval_cmd(
            top_k=args.top_k,
            query_prefix=args.query_prefix,
            load_queries_path=args.load_queries,
            profile=args.profile,
            level=args.level,
            modalities=args.modality,
        )
        return
    if args.command == "reindex-embeddings":
        reindex_embeddings_cmd(batch_size=args.batch_size, model=args.model)
        return
    if args.command == "eval-generation":
        eval_generation_cmd(
            top_k=args.top_k,
            query_prefix=args.query_prefix,
            profile=args.profile,
            model=args.model,
            level=args.level,
        )
        return
    if args.command == "build-memory":
        build_memory_cmd(level=args.level, limit=args.limit, model=args.model)
        return
    if args.command == "memory-stats":
        memory_stats_cmd()
        return
    if args.command == "concepts":
        concepts_cmd(top=args.top)
        return
    if args.command == "timeline":
        timeline_cmd(limit=args.limit)
        return
    if args.command == "usage":
        usage_cmd(clear=args.clear, top=args.top)
        return
    if args.command == "forgetting":
        forgetting_cmd(level=args.level, top=args.top, min_support=args.min_support)
        return
    if args.command == "recall":
        recall_cmd(
            level=args.level,
            top=args.top,
            min_support=args.min_support,
            regenerate=args.regenerate,
        )
        return
    if args.command == "seed-sample":
        seed_sample_cmd(args.workspace_dir, force=args.force, db_path=args.db_path)
        return
    parser.error(f"Unknown command: {args.command}")


def _resolve_ingest_paths(
    explicit_paths: list[Path],
    *,
    env_prefix: str,
    command_name: str,
) -> list[Path]:
    if explicit_paths:
        return [path.expanduser().resolve() for path in explicit_paths]
    env_paths = get_numbered_env_paths(env_prefix)
    if env_paths:
        print(
            f"No explicit paths provided for `{command_name}`. "
            f"Using {len(env_paths)} path(s) from {env_prefix}_* in local environment."
        )
        return env_paths
    return []


if __name__ == "__main__":
    main()
