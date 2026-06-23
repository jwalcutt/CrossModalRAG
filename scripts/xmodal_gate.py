#!/usr/bin/env python3
"""Evaluate the Phase 3 native-embedding gate (pre-committed, see project-scope.md §1a).

Runs retrieval eval over the two cross-modal slices and reports whether the
OCR-text-first shortfall justifies a native (CLIP-class) image-embedding spike:

    GATE: FIRE  when  Recall@K(text-heavy) - Recall@K(visual-heavy) >= 0.30
    GATE: HOLD  otherwise

NOTE: this is only meaningful after Phase 3 step-3 (OCR-text-first ingestion)
exists. Before then the cross-modal fixtures are materialized but not ingested,
so both slices score ~0 and the gate correctly HOLDs.

Authoritative measurement protocol: the native-embedding alternative is *semantic*,
so the OCR-text-first baseline must be measured semantically too (a lexical-only
reading handicaps OCR-text-first and can produce a false FIRE). Embed the corpus
first:

    mem seed-sample --db-path <db>
    CMRAG_DB_PATH=<db> mem reindex-embeddings   # needs the [embeddings] extra
    python scripts/xmodal_gate.py --db-path <db>

This script WARNS when no vectors are stored for the active model (lexical
fallback in effect) so a non-authoritative reading is never mistaken for the gate.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from crossmodalrag.config import get_db_path, load_dotenv  # noqa: E402
from crossmodalrag.db import connect, init_db  # noqa: E402
from crossmodalrag.embed.provider import get_default_provider  # noqa: E402
from crossmodalrag.evaluation import (  # noqa: E402
    XMODAL_GATE_THRESHOLD,
    run_eval,
    xmodal_gate_delta,
    xmodal_gate_fires,
)
from crossmodalrag.retrieve.vector import has_vectors_for_model  # noqa: E402

TEXT_PREFIX = "[sample-xmodal-text]"
VISUAL_PREFIX = "[sample-xmodal-visual]"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report the Phase 3 native-embedding gate for cross-modal retrieval."
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
    return parser


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    db_path = (args.db_path or get_db_path()).expanduser().resolve()

    conn = connect(db_path)
    try:
        init_db(conn)
        provider = get_default_provider()
        semantic = provider is not None and has_vectors_for_model(conn, provider.name)
        text_summary = run_eval(
            conn, top_k=args.top_k, query_prefix=TEXT_PREFIX, profile=args.profile
        )
        visual_summary = run_eval(
            conn, top_k=args.top_k, query_prefix=VISUAL_PREFIX, profile=args.profile
        )
    finally:
        conn.close()

    delta = xmodal_gate_delta(text_summary, visual_summary)
    fires = xmodal_gate_fires(text_summary, visual_summary)

    print(f"DB: {db_path}")
    print(f"profile={args.profile}  top_k={args.top_k}")
    print(
        f"text-heavy   ({TEXT_PREFIX}):   "
        f"queries={text_summary.query_count}  Recall@{args.top_k}={text_summary.recall_at_k:.3f}"
    )
    print(
        f"visual-heavy ({VISUAL_PREFIX}): "
        f"queries={visual_summary.query_count}  Recall@{args.top_k}={visual_summary.recall_at_k:.3f}"
    )
    print(f"delta (text - visual) = {delta:.3f}  (threshold {XMODAL_GATE_THRESHOLD:.2f})")
    print(f"GATE: {'FIRE' if fires else 'HOLD'}")
    if not semantic:
        print(
            "WARNING: no stored vectors for the active embedding model — retrieval fell back to "
            "LEXICAL only. This is NOT the authoritative gate (it handicaps OCR-text-first). "
            "Run `mem reindex-embeddings` (needs the [embeddings] extra) and re-run."
        )
    if text_summary.query_count == 0 and visual_summary.query_count == 0:
        print(
            "note: no cross-modal queries scored — seed the sample data and (post step-3) "
            "ingest the fixtures before reading the gate as a real signal."
        )


if __name__ == "__main__":
    main()
