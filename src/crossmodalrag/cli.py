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
from crossmodalrag.ingest.git import ingest_git
from crossmodalrag.ingest.notes import ingest_notes
from crossmodalrag.memory.concepts import build_concepts
from crossmodalrag.memory.episodes import build_episodes
from crossmodalrag.memory.extract import extract_pending_sources
from crossmodalrag.memory.integrity import (
    count_edges,
    count_nodes_by_level,
    count_nodes_by_type,
    find_dangling_edges,
    find_unsupported_nodes,
)
from crossmodalrag.retrieve.hybrid import DEFAULT_PROFILE, PROFILE_WEIGHTS, retrieve
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


def ask_cmd(
    query: str,
    top_k: int = 5,
    profile: str = DEFAULT_PROFILE,
    explain: bool = False,
    use_llm: bool = True,
    as_json: bool = False,
    debug: bool = False,
) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        hits = retrieve(conn, query=query, top_k=top_k, profile=profile)
    finally:
        conn.close()

    provider = get_default_llm_provider() if use_llm else None
    if provider is not None:
        try:
            gen = synthesize_answer(query, hits, provider)
        except LLMUnavailable as exc:
            print(f"[notice] LLM unavailable, falling back to evidence template: {exc}", file=sys.stderr)
            provider = None
        else:
            if as_json:
                print(json.dumps(generated_answer_to_dict(gen), indent=2))
            else:
                print(format_generated_answer(gen, explain=explain, debug=debug))
            return

    # No LLM (disabled or unavailable): deterministic evidence template.
    if as_json:
        print(
            json.dumps(
                {
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
                            "scores": {
                                "combined": hit.score,
                                "vector": hit.vector_score,
                                "lexical": hit.lexical_score,
                                "recency": hit.recency_score,
                            },
                        }
                        for i, hit in enumerate(hits, start=1)
                    ],
                },
                indent=2,
            )
        )
    else:
        print(format_grounded_answer(query, hits, explain=explain or debug))


def eval_cmd(
    top_k: int = 5,
    query_prefix: str | None = None,
    load_queries_path: Path | None = None,
    profile: str = DEFAULT_PROFILE,
) -> None:
    db_path = get_db_path()
    conn = connect(db_path)
    try:
        init_db(conn)
        loaded = 0
        if load_queries_path is not None:
            queries = load_eval_queries_file(load_queries_path)
            loaded = upsert_eval_queries(conn, queries)
        summary = run_eval(conn, top_k=top_k, query_prefix=query_prefix, profile=profile)
    finally:
        conn.close()

    print(f"Evaluation DB: {db_path}")
    print(f"Retrieval profile: {profile}")
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
    finally:
        conn.close()
    print(f"Reindex DB: {db_path}")
    print(f"Model: {provider.name} (dim={provider.dim})")
    print(f"Chunks embedded this run: {embedded}")
    print(f"Total chunks with current-model embeddings: {total}")


def eval_generation_cmd(
    top_k: int = 5,
    query_prefix: str | None = None,
    profile: str = DEFAULT_PROFILE,
    model: str | None = None,
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
                conn, provider, top_k=top_k, query_prefix=query_prefix, profile=profile
            )
        except LLMUnavailable as exc:
            raise SystemExit(str(exc))
    finally:
        conn.close()

    print(f"Evaluation DB: {db_path}")
    print(f"Model: {summary.model} | profile: {summary.profile}")
    if query_prefix:
        print(f"Query prefix filter: {query_prefix}")
    if summary.query_count == 0:
        print("No evaluation queries found. Load queries into 'queries_eval' and run again.")
        return
    print(f"Queries evaluated: {summary.query_count}")
    print(f"Citation validity: {summary.citation_validity:.3f}")
    print(f"Source-grounding hit: {summary.source_grounding_hit:.3f}")
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
    print(f"Node embeddings: {node_vectors}")
    print("Integrity:")
    print(f"  unsupported nodes (no L0 evidence): {len(unsupported)}")
    print(f"  dangling edges (missing endpoint): {len(dangling)}")
    if unsupported:
        print(f"  unsupported node ids: {unsupported}")
    if dangling:
        print(f"  dangling edge ids: {dangling}")


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
        f"(notes={result.notes_chunks_inserted}, git={result.git_chunks_inserted}); "
        f"eval queries upserted={result.eval_queries_upserted}"
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

    profile_choices = sorted(PROFILE_WEIGHTS)

    p_ask = sub.add_parser("ask", help="Query indexed evidence.")
    p_ask.add_argument("query", type=str)
    p_ask.add_argument("--top-k", type=int, default=5)
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
        choices=["event", "episode", "concept", "all"],
        default="all",
        help="Memory level to build: 'event' (LLM), 'episode' (no LLM), "
        "'concept' (embeddings extra; LLM naming optional), or 'all' (default).",
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
    if args.command == "ask":
        ask_cmd(
            args.query,
            top_k=args.top_k,
            profile=args.profile,
            explain=args.explain,
            use_llm=not args.no_llm,
            as_json=args.as_json,
            debug=args.debug,
        )
        return
    if args.command == "eval":
        eval_cmd(
            top_k=args.top_k,
            query_prefix=args.query_prefix,
            load_queries_path=args.load_queries,
            profile=args.profile,
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
        )
        return
    if args.command == "build-memory":
        build_memory_cmd(level=args.level, limit=args.limit, model=args.model)
        return
    if args.command == "memory-stats":
        memory_stats_cmd()
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
