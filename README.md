# CrossModalRAG

This repository contains a local-first foundation for a cross-modal memory system.

Current scope:
- Ingest markdown notes into SQLite.
- Ingest git commits and diffs into SQLite.
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
- `--explain` prints per-hit score components.
- `--no-llm` skips synthesis and returns the deterministic evidence template.
- `--json` emits a structured answer (stable contract for UIs).
- `--debug` adds retrieval diagnostics plus the raw prompt and model output.

7. Run retrieval evaluation (using seeded sample queries or your own `queries_eval` rows):

```bash
mem eval --top-k 5 --profile relevant
```

8. Evaluate grounded answer quality (citation faithfulness, requires Ollama):

```bash
mem eval-generation --query-prefix "[sample]" --profile relevant
```

This reports `citation_validity` (no hallucinated `[E#]`), `source_grounding_hit` (cites an
expected source), and `abstention_correct` (answers answerable queries, abstains on
unanswerable ones). Queries with empty `expected_source_uris` are treated as negative
(should-abstain) cases. Swap `CMRAG_LLM_MODEL` to compare models.

## CLI Commands

- `mem init-db`
- `mem seed-sample [--workspace-dir PATH] [--force]`
- `mem ingest-notes [<vault_path> ...]` (falls back to `.env` `OBSIDIAN_VAULT_PATH_*`)
- `mem ingest-git [<repo_path> ...] [--max-commits N]` (falls back to `.env` `REPO_PATH_*`)
- `mem ask "<query>" [--top-k N] [--profile balanced|relevant|recent] [--explain] [--no-llm] [--json] [--debug]`
- `mem eval [--top-k N] [--query-prefix PREFIX] [--load-queries PATH.json] [--profile ...]`
- `mem eval-generation [--top-k N] [--query-prefix PREFIX] [--profile ...] [--model ID]` (requires Ollama)
- `mem reindex-embeddings [--batch-size N] [--model ID]` (requires the `[embeddings]` extra)
- `mem build-memory [--level event] [--limit N] [--model ID]` (requires Ollama)
- `mem memory-stats`

## Hierarchical Memory (experimental)

Beyond flat evidence retrieval, CrossModalRAG can build higher-level memory layers on top of the
L0 evidence chunks: L1 atomic events → L2 episodes → L3 concepts (see `project-scope.md` §2). Every
higher-level node is traceable down to its L0 evidence.

Currently implemented: the node/edge substrate and **L1 atomic-event extraction**. `mem build-memory`
uses a local LLM (Ollama) to extract atomic events ("what happened": a decision, learning, fix,
task, or change) from each source, linking each event to its L0 evidence.

```bash
mem build-memory --limit 50    # extract L1 events for up to 50 sources (resumable)
mem memory-stats               # node/edge counts + structural integrity
```

Extraction is deterministic and incremental: a source is re-processed only when its content, the
model, or the prompt version changes — re-running on unchanged data is a no-op. It uses
`CMRAG_EXTRACT_MODEL` (default `llama3.2`, kept separate from the synthesis model so bulk extraction
stays fast); `--model` overrides per run.

## Synthetic Sample Seed Workflow

Use `mem seed-sample` to create a tiny deterministic sample vault + sample git repo and ingest them into an isolated sample DB.

- Creates a local workspace at `./data/sample-seed-workspace` by default
- Writes to a separate temp sample DB by default (does not modify your main `./data/memory.db`)
- Seeds synthetic markdown notes and git commits (no personal data)
- Populates namespaced sample rows in `queries_eval` for future eval/smoke tests
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
