#!/usr/bin/env python3
"""Evaluate the distillation gate

A distilled (compact) representation of L2/L3 memory nodes is ADOPTED only when it preserves
retrieval quality within EPSILON of the full nodes AND meets the size target:

    GATE: FIRE  when  Recall@K(full) - Recall@K(distilled) <= EPSILON   (default 0.05)
                AND   distilled_size / full_size           <= RATIO     (default 0.50)
    GATE: HOLD  otherwise

The semantics are INVERTED versus the xmodal gate: xmodal FIRES on a *large* gap (it
justifies a measurement spike), whereas the distillation gate FIRES (adopts) only when the recall
loss is *small enough* and compression is achieved.

NOTE: this is only meaningful once a later step builds the distilled retrieval path. Until
then there is no distilled summary to compare against the full nodes, so this script reads the full
baseline on the [sample-drift] slice, prints a "distillation not yet built" banner, and HOLDs
(mirroring the xmodal "no signal yet" precedent). The reading is NON-AUTHORITATIVE until then.

Authoritative measurement protocol (once distillation exists): the distilled path is *semantic*, so
the full baseline must be measured semantically too. Embed the corpus first:

    mem seed-sample --db-path <db>
    CMRAG_DB_PATH=<db> mem build-memory          # derive L1-L3 nodes
    CMRAG_DB_PATH=<db> mem reindex-embeddings     # needs the [embeddings] extra
    python scripts/distill_gate.py --db-path <db>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from crossmodalrag.config import (  # noqa: E402
    get_db_path,
    get_distill_compression_ratio,
    get_distill_epsilon,
    load_dotenv,
)
from crossmodalrag.db import connect, init_db  # noqa: E402
from crossmodalrag.evaluation import (  # noqa: E402
    distill_gate_delta,
    distill_gate_fires,
    distilled_compression_ratio,
    run_distilled_eval,
    run_eval,
)

DRIFT_PREFIX = "[sample-drift]"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report the distillation gate (retrieval-preserving compression)."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Database path (defaults to CMRAG_DB_PATH or ./data/memory.db).",
    )
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument(
        "--profile",
        type=str,
        default="relevant",
        help="Hybrid retrieval profile (vector/lexical/recency blend).",
    )
    parser.add_argument(
        "--level",
        type=str,
        default="concept",
        choices=["evidence", "event", "episode", "concept"],
        help="Retrieval level for the full-node baseline (the distilled path stands in for these).",
    )
    parser.add_argument(
        "--query-prefix",
        type=str,
        default=DRIFT_PREFIX,
        help=f"Eval slice to measure (default {DRIFT_PREFIX}).",
    )
    return parser


def _distilled_eval(conn, *, top_k, query_prefix, profile, level):
    """Run the distilled retrieval path. Returns (EvalSummary, compression_ratio) or None.

    Returns None (→ HOLD banner) when nothing has been distilled at this level yet (run
    `mem build-memory --level distill` first). Distilled retrieval only applies to L2/L3
    stand-ins, so an evidence-level request also returns None.
    """
    if level not in ("episode", "concept"):
        return None
    has_distilled = conn.execute(
        "SELECT 1 FROM distilled_nodes LIMIT 1"
    ).fetchone()
    if has_distilled is None:
        return None
    summary = run_distilled_eval(
        conn, top_k=top_k, query_prefix=query_prefix, profile=profile, level=level
    )
    ratio = distilled_compression_ratio(conn, level=level)
    return summary, ratio


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    db_path = (args.db_path or get_db_path()).expanduser().resolve()
    epsilon = get_distill_epsilon()
    target_ratio = get_distill_compression_ratio()

    conn = connect(db_path)
    try:
        init_db(conn)
        full_summary = run_eval(
            conn,
            top_k=args.top_k,
            query_prefix=args.query_prefix,
            profile=args.profile,
            level=args.level,
        )
        distilled = _distilled_eval(
            conn,
            top_k=args.top_k,
            query_prefix=args.query_prefix,
            profile=args.profile,
            level=args.level,
        )
    finally:
        conn.close()

    print(f"DB: {db_path}")
    print(f"profile={args.profile}  top_k={args.top_k}  level={args.level}  slice={args.query_prefix}")
    print(
        f"full nodes:  queries={full_summary.query_count}  "
        f"Recall@{args.top_k}={full_summary.recall_at_k:.3f}"
    )

    if distilled is None:
        print("distilled:   not built; no compact representation to compare")
        print(
            f"gate budget: epsilon={epsilon:.3f}  target compression ratio<={target_ratio:.2f}"
        )
        print("GATE: HOLD")
        print(
            "note: distillation is not yet implemented (this is the scaffolding step). The reading "
            "is NON-AUTHORITATIVE; a later step builds the distilled retrieval path and the "
            "gate becomes meaningful."
        )
        return

    distilled_summary, compression_ratio = distilled
    delta = distill_gate_delta(full_summary, distilled_summary)
    fires = distill_gate_fires(
        full_summary,
        distilled_summary,
        compression_ratio=compression_ratio,
        epsilon=epsilon,
        target_ratio=target_ratio,
    )
    print(
        f"distilled:   queries={distilled_summary.query_count}  "
        f"Recall@{args.top_k}={distilled_summary.recall_at_k:.3f}"
    )
    print(
        f"recall lost (full - distilled) = {delta:.3f}  (epsilon {epsilon:.3f})  "
        f"compression ratio = {compression_ratio:.3f}  (target <= {target_ratio:.2f})"
    )
    print(f"GATE: {'FIRE' if fires else 'HOLD'}")


if __name__ == "__main__":
    main()
