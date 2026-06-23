# CrossModalRAG

This repository contains a local-first foundation for a cross-modal memory system.

Current scope:

- Ingest markdown notes into SQLite.
- Ingest git commits and diffs into SQLite.
- Ingest PDF text (per-page, with page locators) into SQLite — optional `[pdf]` extra.
- Create structure-aware searchable chunks from evidence.
- Query with a hybrid (semantic vector + lexical + recency) retriever and get cited evidence.
- Optional local embeddings via `fastembed` (no torch); falls back to lexical when not installed.
- Synthesize grounded answers with a local LLM (Ollama) constrained to and citing the evidence,
abstaining when evidence is weak; falls back to a deterministic template when Ollama is absent.

## Quickstart

1. Create a virtual environment and install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

For semantic (vector) retrieval, install the optional embeddings extra. The core stays
dependency-free; without this extra everything still works using lexical retrieval only.

```bash
pip install -e ".[embeddings]"   # adds fastembed (ONNX, local, no torch) + numpy
```

2. Initialize the local database:

```bash
mem init-db
```

Optional: seed deterministic synthetic notes + git history for smoke testing:

```bash
mem seed-sample
```

3. Ingest notes:

```bash
mem ingest-notes /path/to/obsidian/vault
```

Or ingest multiple vaults in one command:

```bash
mem ingest-notes /path/to/vault-a /path/to/vault-b
```

If you omit paths, `mem ingest-notes` will use all `OBSIDIAN_VAULT_PATH_<n>` values from your local `.env`.

4. Ingest git history:

```bash
mem ingest-git /path/to/repo --max-commits 300
```

Or ingest multiple repos in one command:

```bash
mem ingest-git /path/to/repo-a /path/to/repo-b --max-commits 300
```

If you omit paths, `mem ingest-git` will use all `REPO_PATH_<n>` values from your local `.env`.

### Ingest PDFs (optional, cross-modal)

PDF ingestion is **text-first**: each page's extractable text becomes searchable chunks that carry a
1-based page locator, so evidence can be cited as `file.pdf p.N`. It requires the optional `[pdf]`
extra (`pip install -e ".[pdf]"`); without it the command exits with an install hint and the core
stays dependency-free.

```bash
mem ingest-pdf /path/to/file.pdf /path/to/dir-of-pdfs
```

A path may be a single `.pdf` file or a directory (searched recursively). One source row is created
per file; re-ingesting an unchanged file is a no-op (the fingerprint folds in the extractor version,
so an extractor upgrade re-derives intentionally). Pages with no extractable text (e.g. scanned,
image-only pages) register the source but produce no chunks — image OCR is a later Phase 3 step. If
you omit paths, `mem ingest-pdf` uses all `PDF_PATH_<n>` values from your local `.env`.

### Forcing a re-chunk (after chunker changes)

Ingestion is idempotent: a source whose content fingerprint is unchanged is skipped, and its
existing chunks are left untouched. This means improvements to the chunking logic do **not**
apply to already-ingested sources until their content changes.

There is currently no in-place `--force` flag on the ingest commands. To re-chunk existing
data with the latest chunkers, rebuild the database from scratch:

```bash
# Rebuild the default ./data/memory.db in place
rm -f ./data/memory.db ./data/memory.db-wal ./data/memory.db-shm
mem init-db
mem ingest-notes   # explicit paths, or OBSIDIAN_VAULT_PATH_* from .env
mem ingest-git     # explicit paths, or REPO_PATH_* from .env
```

To rebuild into a separate database without disturbing your current one, point `CMRAG_DB_PATH`
at a fresh path first, then ingest there and compare before swapping:

```bash
export CMRAG_DB_PATH=/absolute/path/to/memory-rechunked.db
mem init-db
mem ingest-notes
mem ingest-git
```

Note: a full rebuild also clears the `queries_eval` table. If you have custom evaluation
queries, re-add them with `mem eval --load-queries file.json` (see the query file format below)
after re-ingesting.

5. (Optional) Build semantic embeddings for ingested chunks:

```bash
mem reindex-embeddings
```

This embeds any chunks that don't yet have a vector for the active model (resumable and
idempotent). It requires the `[embeddings]` extra. Ingestion also embeds inline when the extra
is installed, so `reindex-embeddings` is mainly for backfilling existing data or after changing
the model. Vectors are tagged with the model that produced them; changing `CMRAG_EMBED_MODEL`
means you should re-run this command.

6. Ask a question:

```bash
mem ask "Why did I change the parser?" --top-k 5 --profile relevant --explain
```

By default `mem ask` synthesizes a grounded answer with a local LLM via Ollama, constrained to
the retrieved evidence and citing it inline as `[E#]`. If retrieval is too weak (top score below
`CMRAG_MIN_EVIDENCE_SCORE`) it abstains instead of guessing. If Ollama is unreachable it
automatically falls back to the deterministic evidence template.

Requires [Ollama](https://ollama.com) running locally with a model pulled
(`ollama pull gemma4`). Swap models anytime via `CMRAG_LLM_MODEL`.

- `--profile` selects the hybrid retrieval blend of semantic / lexical / recency:
  - `balanced` (default): 0.55 vector + 0.30 lexical + 0.15 recency
  - `relevant`: 0.70 vector + 0.25 lexical + 0.05 recency
  - `recent`: 0.35 vector + 0.20 lexical + 0.45 recency
- `--level` chooses the retrieval level: `evidence` (default, L0 chunks) or a memory level
  (`event`/`episode`/`concept`). At a memory level, `ask` retrieves the matching nodes, prints them,
  then drills them down to their L0 evidence and answers grounded in (and citing) that L0 — so
  provenance holds regardless of entry level. Memory-level retrieval needs the hierarchy built
  (`mem build-memory`) and benefits from node embeddings (`mem reindex-embeddings`); without the
  embeddings extra it ranks nodes lexically.
- `--explain` prints per-hit score components.
- `--no-llm` skips synthesis and returns the deterministic evidence template.
- `--json` emits a structured answer (stable contract for UIs; includes `matched_nodes` at memory levels).
- `--debug` adds retrieval diagnostics plus the raw prompt and model output.

```bash
mem ask "what are the themes of this project?" --level concept   # retrieve concepts, answer from their L0 evidence
```

7. Run retrieval evaluation (using seeded sample queries or your own `queries_eval` rows):

```bash
mem eval --top-k 5 --profile relevant                 # evidence-level recall/MRR
mem eval --top-k 5 --level concept                    # drill-down recall via the concept layer
```

8. Evaluate grounded answer quality (citation faithfulness, requires Ollama):

```bash
mem eval-generation --query-prefix "[sample]" --profile relevant            # flat L0 baseline
mem eval-generation --query-prefix "[sample-synth]" --level concept          # synthesis at a memory level
```

This reports `citation_validity` (no hallucinated `[E#]`), `source_grounding_hit` (cites at
least one expected source), `source_coverage` (fraction of an answerable query's expected
sources actually cited — rewards multi-source synthesis), and `abstention_correct` (answers
answerable queries, abstains on unanswerable ones). Queries with empty `expected_source_uris`
are treated as negative (should-abstain) cases. Swap `CMRAG_LLM_MODEL` to compare models.

`--level` runs the same citation-faithfulness eval over a memory level (`event`/`episode`/
`concept`): it retrieves matching nodes, drills them down to their L0 evidence, and synthesizes
from that L0 — so answers still cite L0 `[E#]` regardless of entry level. Compare a memory level
against `--level evidence` to gauge the synthesis benefit. The seeded `[sample-synth]` queries
have multi-source gold for exactly this; the `[sample]` queries are single-source specific-fact
checks for measuring no regression.

## CLI Commands

- `mem init-db`
- `mem seed-sample [--workspace-dir PATH] [--force]`
- `mem ingest-notes [<vault_path> ...]` (falls back to `.env` `OBSIDIAN_VAULT_PATH_*`)
- `mem ingest-git [<repo_path> ...] [--max-commits N]` (falls back to `.env` `REPO_PATH_*`)
- `mem ingest-pdf [<path> ...]` (file or directory; falls back to `.env` `PDF_PATH_*`; requires the `[pdf]` extra)
- `mem ask "<query>" [--top-k N] [--level evidence|event|episode|concept] [--profile balanced|relevant|recent] [--explain] [--no-llm] [--json] [--debug]`
- `mem eval [--top-k N] [--query-prefix PREFIX] [--load-queries PATH.json] [--profile ...] [--level ...]`
- `mem eval-generation [--top-k N] [--query-prefix PREFIX] [--profile ...] [--level evidence|event|episode|concept] [--model ID]` (requires Ollama)
- `mem reindex-embeddings [--batch-size N] [--model ID]` (requires the `[embeddings]` extra)
- `mem build-memory [--level event|episode|concept|graph|all] [--limit N] [--model ID]` (events/concept-naming use Ollama; concepts need the `[embeddings]` extra; graph needs neither)
- `mem memory-stats`
- `mem concepts [--top N]` (L3 concepts by centrality)
- `mem timeline [--limit N]` (L2 episodes, oldest first)

## Hierarchical Memory (experimental)

Beyond flat evidence retrieval, CrossModalRAG can build higher-level memory layers on top of the  
L0 evidence chunks: L1 atomic events → L2 episodes → L3 concepts . Every higher-level node is traceable down to its L0 evidence.

Currently implemented: the node/edge substrate plus **L1 atomic-event extraction**, **L2 episode
grouping**, and **L3 concept clustering**. `mem build-memory` derives all three:

- **L1 events** (requires Ollama): a local LLM extracts atomic events ("what happened": a decision,
  learning, fix, task, or change) from each source, linking each event to its L0 evidence.
- **L2 episodes** (no LLM): events are grouped into "sessions of related work" by project (git repo
  / note folder) and time gap — a new episode starts when consecutive events in a project are more
  than `CMRAG_EPISODE_GAP_HOURS` apart (default 24). Each episode links to its member events.
- **L3 concepts** (requires the `[embeddings]` extra; LLM naming optional): events are clustered by
  semantic similarity (cosine ≥ `CMRAG_CONCEPT_SIM_THRESHOLD`, default 0.60) into recurring topics
  that span episodes. Each new concept is named by the local LLM (`CMRAG_EXTRACT_MODEL`, temp 0)
  with a deterministic fallback when Ollama is unavailable.
- **Graph** (no LLM/embeddings): computes PageRank **centrality** per node (an importance signal
  stored on each node) and **concept co-occurrence** links — two concepts get a `relates_to` edge
  (weighted by shared-episode count) when they have events in the same episode.

Every higher-level node drills down to its L0 evidence through its members.

```bash
mem build-memory --limit 50          # L1 events (up to 50 sources) + L2 episodes + L3 concepts + graph
mem build-memory --level event       # only L1 events (LLM)
mem build-memory --level episode     # only L2 episodes (no Ollama needed)
mem build-memory --level concept     # only L3 concepts (needs the embeddings extra; run reindex-embeddings first)
mem build-memory --level graph       # only centrality + concept co-occurrence (no LLM/embeddings)
mem memory-stats                     # node/edge counts, co-occurrence edges, top central nodes, integrity
```

Once built and embedded (`mem reindex-embeddings` now also embeds memory nodes), the hierarchy is
queryable: `mem ask --level concept|episode|event` retrieves at that level and answers grounded in
the drilled-down L0 evidence, and `mem concepts` / `mem timeline` browse the concept and episode
layers. Node ranking blends semantic + lexical + recency + centrality.

All layers are deterministic and incremental: L1 sources are re-extracted only when content,
model, or prompt version changes; L2/L3 are reconciled by membership so re-running on unchanged
data is a no-op (and concepts are not re-named). L1 uses `CMRAG_EXTRACT_MODEL` (default `llama3.2`,
separate from the synthesis model so bulk extraction stays fast); `--model` overrides per run.

## Synthetic Sample Seed Workflow

Use `mem seed-sample` to create a tiny deterministic sample vault + sample git repo and ingest them into an isolated sample DB.

- Creates a local workspace at `./data/sample-seed-workspace` by default
- Writes to a separate temp sample DB by default (does not modify your main `./data/memory.db`)
- Seeds synthetic markdown notes and git commits (no personal data)
- Populates namespaced sample rows in `queries_eval`: `[sample]` single-source specific-fact
  queries (incl. one negative/abstain case), `[sample-synth]` multi-source synthesis queries, and
  (Phase 3 scaffolding) `[sample-xmodal-text]` / `[sample-xmodal-visual]` cross-modal slices
- Materializes tiny synthetic cross-modal fixtures (a 1-page PDF + two PNGs) under the sample vault.
  These are **not yet ingested** (image/PDF ingestion lands in later Phase 3 steps), so the
  `[sample-xmodal-*]` queries score ~0 today — an intentional baseline. The two slices are designed
  so an OCR/PDF-text-first strategy should answer the *text-heavy* slice but fail the *visual-heavy*
  one (a layout/colour-only diagram), which is what the pre-committed native-embedding gate measures
  (`python scripts/xmodal_gate.py`). Regenerate the fixtures with `scripts/generate_xmodal_fixtures.py`.
- Safe to re-run; unchanged content is reused and ingestion remains idempotent

Run the sample retrieval benchmark:

```bash
mem eval --query-prefix "[sample]" --top-k 5
```

Use `--force` to rebuild the sample workspace directory from scratch.
Use `--db-path` if you want the sample dataset in a specific non-main database path.

## Data Location

By default, data is stored at:

- `./data/memory.db`

Set `CMRAG_DB_PATH` to override:

```bash
export CMRAG_DB_PATH=/absolute/path/to/memory.db
```

Select the embedding model (defaults to `BAAI/bge-small-en-v1.5`, 384-dim):

```bash
export CMRAG_EMBED_MODEL=BAAI/bge-small-en-v1.5
```

Configure the local LLM used for answer synthesis (defaults shown):

```bash
export CMRAG_LLM_PROVIDER=ollama
export CMRAG_LLM_MODEL=gemma4              # swap for any model you have in `ollama list`
export CMRAG_LLM_BASE_URL=http://localhost:11434
export CMRAG_LLM_TIMEOUT=120
export CMRAG_MIN_EVIDENCE_SCORE=0.15       # abstain below this top retrieval score
export CMRAG_EXTRACT_MODEL=llama3.2        # model for `mem build-memory` event extraction
export CMRAG_EPISODE_GAP_HOURS=24          # L2 episode session gap (deterministic, no LLM)
export CMRAG_CONCEPT_SIM_THRESHOLD=0.60    # L3 concept clustering cosine threshold (embeddings extra)
```

See `.env.example` for the full list of supported variables.

## Evaluation Query File Format (`mem eval --load-queries`)

Use a JSON array of rows:

```json
[
  {
    "query_text": "What fixed the parser bounds check bug?",
    "expected_source_uris": [
      "/abs/path/to/repo@abc123",
      "/abs/path/to/note/http-parser.md"
    ]
  }
]
```

`mem eval --load-queries file.json` upserts rows into `queries_eval` and then runs metrics (`Recall@K`, `MRR@K`, and an approximate citation hit-rate based on the top retrieved source).