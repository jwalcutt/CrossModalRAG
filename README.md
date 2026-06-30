# CrossModalRAG

This repository contains a local-first foundation for a cross-modal memory system.

Current scope:

- Ingest markdown notes into SQLite.
- Ingest git commits and diffs into SQLite.
- Ingest PDF text (per-page, with page locators) into SQLite — optional `[pdf]` extra.
- Ingest image/diagram text via local OCR (with an OCR-confidence signal) — optional `[ocr]` extra.
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
image-only pages) register the source but produce no chunks — use image OCR (below) for those. If
you omit paths, `mem ingest-pdf` uses all `PDF_PATH_<n>` values from your local `.env`.

### Ingest images / diagrams (optional, OCR)

Image ingestion is **OCR-text-first**: the recognized text of each image becomes searchable chunks
tagged `modality=ocr` and carrying an `ocr_confidence` signal (low-confidence OCR is weak evidence).
It requires the optional `[ocr]` extra (`pip install -e ".[ocr]"`) **and** a local
[Tesseract](https://github.com/tesseract-ocr/tesseract) binary (e.g. `brew install tesseract`);
without them the command exits with a hint and the core stays dependency-free.

```bash
mem ingest-images /path/to/diagram.png /path/to/dir-of-images
```

A path may be a single image (`.png/.jpg/.jpeg/.gif/.bmp/.tif/.tiff/.webp`) or a directory (searched
recursively; non-image files are ignored). One source row per image; re-ingesting an unchanged image
is a no-op (the fingerprint folds in the OCR engine version). An image whose OCR yields no text
registers the source with no chunks. Purely visual content (a diagram whose meaning is in its layout,
not its words) is intentionally hard to retrieve from OCR text alone — measuring that gap is what
`scripts/xmodal_gate.py` is for. If you omit paths, `mem ingest-images` uses all `IMAGE_PATH_<n>`
values from your local `.env`.

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

- `--profile` selects the hybrid retrieval blend of semantic / lexical / recency / usage:
  - `balanced` (default): 0.55 vector + 0.30 lexical + 0.15 recency
  - `relevant`: 0.70 vector + 0.25 lexical + 0.05 recency
  - `recent`: 0.35 vector + 0.20 lexical + 0.45 recency
  - `usage`: 0.55 vector + 0.25 lexical + 0.05 recency + **0.15 usage** (rehearsal strength) — an
    **opt-in** time/usage-aware profile that promotes memories you've used recently/often. The usage
    term is 0 in every other profile, so they are unchanged. It re-ranks only already-relevant
    candidates (never surfaces irrelevant ones) and needs embeddings + a usage history
    (`CMRAG_USAGE_HALFLIFE_DAYS`, `CMRAG_USAGE_SATURATION`). `--explain`/`--json` expose a `usage`
    score component.
- **Usage tracking is opt-in and private.** The `usage` profile's history comes from real `mem ask`
  interactions, but tracking is **off by default**. Enable it with `CMRAG_USAGE_TRACKING=on`, or
  per-call with `mem ask … --track` (logs `retrieval_hit` for the evidence shown) and
  `mem ask … --accept` (also logs `accepted_answer` for the cited evidence; implies `--track`).
  `mem ask … --level …` additionally logs an `open` event for the memory nodes drilled into.
  `--no-track` suppresses logging for a call. Only the target id + event type + time are stored —
  **never your query text** — and everything stays local. Inspect with `mem usage`; wipe with
  `mem usage --clear`.
- **Forgetting risk** (`mem forgetting`): surfaces important-but-stale memories — "what am I likely
  forgetting that's still relevant?" Each memory node is scored `risk = importance × staleness`, where
  importance is its graph centrality (run `mem build-memory` first) and staleness grows with time since
  you last touched it (a recent `retrieval`/`open` via tracking, or its content age) — so rehearsing a
  memory lowers its risk. Every item is grounded to its L0 evidence and shows its
  importance/staleness/confidence components. Use `--level all` to rank across events/episodes/concepts.
  Read-only.
- **Active recall** (`mem recall`): turns the highest forgetting-risk memories into grounded study
  cards — a question plus a one-sentence answer drawn **strictly from that memory's L0 evidence**.
  Cards are generated by the local LLM (`CMRAG_EXTRACT_MODEL`, temp 0) and **cached** (regenerated only
  when the underlying memory changes, or with `--regenerate`); if Ollama is unavailable it falls back
  to a deterministic template question + an evidence excerpt, so it never fails. Each card cites its
  evidence. `--level all` covers events/episodes/concepts.
- **Concept drift** (`mem drift`): shows how your L3 concepts have **moved over time** — for each
  concept, its member events are bucketed into time windows (`CMRAG_DRIFT_WINDOW_DAYS`, default 30),
  a per-window prototype (centroid) is computed, and the drift between consecutive windows is scored
  as `1 − cosine`. Concepts re-engaged after an empty window are flagged as **relearning**. Each item
  shows window count, span, support, confidence, and a grounding URI. Build the snapshots first with
  `mem build-memory --level drift` (deterministic, no LLM; needs the `[embeddings]` extra); `mem drift`
  is a read-only view. Note timestamps drive the windows — see "date-aware note dates" below.
  `mem drift --json` emits a stable contract (per concept: `concept_id`, `overall_drift`, `relearning`,
  a grounding URI, and a `windows` array — the movement trajectory for a UI to plot).
- **Distilled stand-ins** (`mem distill`): lists the compact, retrieval-preserving representations
  built by `mem build-memory --level distill` — for each L2/L3 node, its summary, the kept (core) vs
  full L0 evidence counts, the achieved compression ratio, confidence, and a grounding URI. Read-only.
  `mem distill --json` adds a stable per-node contract plus the per-level `overall_compression_ratio`.
  Whether to actually retrieve via these stand-ins in production is decided by the distillation gate
  (`scripts/distill_gate.py`), not enabled by default — see the distillation gate section below.
- `--level` chooses the retrieval level: `evidence` (default, L0 chunks) or a memory level
  (`event`/`episode`/`concept`). At a memory level, `ask` retrieves the matching nodes, prints them,
  then drills them down to their L0 evidence and answers grounded in (and citing) that L0 — so
  provenance holds regardless of entry level. Memory-level retrieval needs the hierarchy built
  (`mem build-memory`) and benefits from node embeddings (`mem reindex-embeddings`); without the
  embeddings extra it ranks nodes lexically.
- `--explain` prints per-hit score components.
- `--no-llm` skips synthesis and returns the deterministic evidence template.
- `--json` emits a structured answer (stable contract for UIs; includes `matched_nodes` at memory levels).
  Each evidence entry also carries cross-modal provenance: `modality`, a rendered `locator`
  (e.g. `spec.pdf p.4`), and `page` / `ocr_confidence` when applicable (additive; existing fields
  unchanged).
- `--modality` restricts evidence to one or more modalities (repeatable): `text` (notes), `code`
  (git), `pdf`, `image` (OCR'd images). Citations render the modality and locator inline.
- `--debug` adds retrieval diagnostics plus the raw prompt and model output.

```bash
mem ask "what are the themes of this project?" --level concept   # retrieve concepts, answer from their L0 evidence
mem ask "what does the spec say about rate limits?" --modality pdf --json   # only PDF evidence, cited as file.pdf p.N
```

Near-identical evidence is de-duplicated across modalities (e.g. an OCR'd screenshot of a note and
the note itself) so the same content isn't cited twice (`CMRAG_DEDUPE_THRESHOLD`, default 0.95).

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
- `mem doctor [--json]` (read-only health check: DB, installed extras, Ollama reachability, active models, config file, configured connectors, memory build/integrity)
- `mem sync [--max-commits N] [--only notes|git|pdf|image ...] [--json]` (incrementally re-ingest every connector configured in `.env`/the config file; idempotent — only changed sources are re-chunked)
- `mem backup [<dest>]` (write a consistent single-file copy of the local DB; WAL-safe)
- `mem restore <src> [--force]` (replace the local DB with a backup; destructive — `--force` required to overwrite an existing DB)
- `mem serve [--host 127.0.0.1] [--port 8765]` (local read-only HTTP API serving the JSON contracts; requires the `[ui]` extra; localhost-only by default)
- `mem seed-sample [--workspace-dir PATH] [--force]`
- `mem ingest-notes [<vault_path> ...]` (falls back to `.env` `OBSIDIAN_VAULT_PATH_*`)
- `mem ingest-git [<repo_path> ...] [--max-commits N]` (falls back to `.env` `REPO_PATH_*`)
- `mem ingest-pdf [<path> ...]` (file or directory; falls back to `.env` `PDF_PATH_*`; requires the `[pdf]` extra)
- `mem ingest-images [<path> ...]` (file or directory; falls back to `.env` `IMAGE_PATH_*`; requires the `[ocr]` extra + a tesseract binary)
- `mem ask "<query>" [--top-k N] [--level evidence|event|episode|concept] [--profile balanced|relevant|recent|usage] [--modality text|code|pdf|image ...] [--explain] [--no-llm] [--json] [--debug] [--track|--no-track] [--accept]`
- `mem usage [--clear] [--top N] [--json]` (local usage-tracking stats; `--clear` wipes the history)
- `mem forgetting [--level concept|episode|event|all] [--top N] [--min-support N] [--json]` ("what am I likely forgetting?" — important-but-stale memories, grounded to evidence)
- `mem recall [--level concept|episode|event|all] [--top N] [--min-support N] [--regenerate] [--json]` (grounded active-recall study cards for the highest forgetting-risk memories)
- `mem drift [--top N] [--min-support N] [--json]` (concept drift over time windows; read-only — run `mem build-memory --level drift` first, needs the `[embeddings]` extra)
- `mem distill [--top N] [--json]` (list distilled node stand-ins: core/full evidence + compression ratio; read-only — run `mem build-memory --level distill` first)
- `mem eval [--top-k N] [--query-prefix PREFIX] [--load-queries PATH.json] [--profile ...] [--level ...] [--modality text|code|pdf|image ...] [--json]`
- `mem eval-generation [--top-k N] [--query-prefix PREFIX] [--profile ...] [--level evidence|event|episode|concept] [--model ID]` (requires Ollama)
- `mem reindex-embeddings [--batch-size N] [--model ID]` (requires the `[embeddings]` extra)
- `mem build-memory [--level event|episode|concept|graph|drift|distill|all] [--limit N] [--model ID]` (events/concept-naming use Ollama; concepts + drift + distill need the `[embeddings]` extra; episode/graph need neither)
- `mem memory-stats [--json]` (node/edge counts, integrity, plus distilled-node + drift-snapshot counts)
- `mem concepts [--top N] [--json]` (L3 concepts by centrality)
- `mem timeline [--limit N] [--json]` (L2 episodes, oldest first)

### Operations (sync, doctor, progress, exit codes)

- **`mem sync`** re-ingests every connector configured in your `.env` (`OBSIDIAN_VAULT_PATH_*`,
  `REPO_PATH_*`, `PDF_PATH_*`, `IMAGE_PATH_*`) in one pass. It is **incremental and idempotent**:
  ingestion fingerprint-skips unchanged sources, so a second run re-chunks only what changed. PDF/image
  connectors are **skipped with a note** (not an error) when their extra is absent, and a bad path is
  reported per-connector without aborting the rest of the sync. `--only` restricts to specific
  connectors; `--json` emits a summary.
- **`mem doctor`** is a read-only health check — DB path/size, which optional extras are installed,
  whether Ollama is reachable, the active embed/LLM/extract models, configured connector counts, and
  memory build + integrity. Use it to diagnose "why is retrieval lexical-only?" or "why did `ask`
  fall back to the template?".
- **Progress.** Long operations (`ingest-*`, `sync`, `reindex-embeddings`, `build-memory`) print a
  progress line to **stderr only when it is an interactive terminal**, so piping or `--json` output is
  never polluted.
- **Exit codes.** `0` success; `1` an expected, reported failure (printed as `error: <message>`, no
  stack trace); `2` a command-line usage error.
- **Backup / restore.** `mem backup [<dest>]` writes a WAL-safe single-file copy of the DB (default:
  alongside it with a timestamp). `mem restore <src>` replaces the active DB from a backup; it is
  **destructive** and refuses to overwrite an existing DB unless you pass `--force`.

### Config file (optional)

Beyond `.env`, an optional **TOML** config file can hold connector paths and retrieval defaults.
CrossModalRAG looks for `$CMRAG_CONFIG`, else `./crossmodalrag.toml`. Copy `crossmodalrag.toml.example`
to get started. Resolution precedence is always **CLI flag > environment/`.env` > config file > built-in
default**, so the config never overrides something you pass explicitly, and the eval baselines (which
pass an explicit profile) do not move.

```toml
[connectors]                       # used by `mem sync` / `mem ingest-*` fallback
notes = ["/path/to/vault"]
git   = ["/path/to/repo"]

[retrieval]                        # applied when you omit the flag
profile = "relevant"               # balanced | relevant | recent | usage
top_k = 5
```

(Requires Python 3.11+ for the stdlib `tomllib` parser; the core install stays dependency-free.)

### JSON output contracts

The `--json` modes are **stable, machine-readable contracts** intended for tooling and (future) UI
integration — `ask`, `eval`, `concepts`, `timeline`, `memory-stats`, `forgetting`, `recall`, `usage`,
`drift`, and `distill` all support `--json`. Each contract is **additive-only**: field names and shapes
are kept backward-compatible, and changes only *add* keys (never rename or remove). Every contract that
returns memory items carries stable identifiers (`node_id`) and provenance (`evidence_source_uris` /
evidence ids + L0 locators) so a consumer can drill back down to the source. The shapes are owned by the
library (the `*_to_dict` / `list_*` / `memory_stats` helpers) and pinned by `tests/test_json_contracts.py`,
so the CLI and any future API/UI render exactly the same payloads.

### Local API (`mem serve`)

The same JSON contracts are available over a **local, read-only HTTP API** — the boundary a web UI or
an Obsidian plugin can call without re-implementing retrieval. It requires the opt-in `[ui]` extra
(`pip install -e ".[ui]"`, adds FastAPI + uvicorn) and **binds `127.0.0.1` by default** (no auth on
loopback; nothing leaves the machine). Without the extra, `mem serve` exits with an install hint.

```bash
pip install -e ".[ui]"
mem serve                       # http://127.0.0.1:8765  (use --host/--port to change; Ctrl-C to stop)
curl -s localhost:8765/health
curl -s "localhost:8765/ask?q=why+did+I+change+the+parser&use_llm=true"
```

All routes are **GET, read-only** (no writes; `/ask` does not record usage): `/health`, `/ask`,
`/concepts`, `/timeline`, `/memory-stats`, `/forgetting`, `/recall`, `/drift`, `/distill`, `/usage` —
each returns exactly the corresponding `--json` payload. Interactive docs are at `/docs`. Binding to a
non-loopback `--host` exposes the unauthenticated API on your network and is warned against.

## Hierarchical Memory (experimental)

Beyond flat evidence retrieval, CrossModalRAG can build higher-level memory layers on top of the  
L0 evidence chunks: L1 atomic events → L2 episodes → L3 concepts . Every higher-level node is traceable down to its L0 evidence.

Currently implemented: the node/edge substrate plus **L1 atomic-event extraction**, **L2 episode
grouping**, and **L3 concept clustering**. `mem build-memory` derives all three:

- **L1 events** (requires Ollama): a local LLM extracts atomic events ("what happened": a decision,
  learning, fix, task, or change) from each source — including PDF pages and OCR'd images — linking
  each event to its L0 evidence (so the hierarchy spans modalities and drills back to a cited
  page/region locator).
- **L2 episodes** (no LLM): events are grouped into "sessions of related work" by project (git repo
  / containing folder for notes, PDFs, and images) and time gap — a new episode starts when
  consecutive events in a project are more than `CMRAG_EPISODE_GAP_HOURS` apart (default 24). A folder
  mixing notes, PDFs, and screenshots forms one cross-modal episode. Each episode links to its members.
- **L3 concepts** (requires the `[embeddings]` extra; LLM naming optional): events are clustered by
  semantic similarity (cosine ≥ `CMRAG_CONCEPT_SIM_THRESHOLD`, default 0.60) into recurring topics
  that span episodes. Each new concept is named by the local LLM (`CMRAG_EXTRACT_MODEL`, temp 0)
  with a deterministic fallback when Ollama is unavailable.
- **Graph** (no LLM/embeddings): computes PageRank **centrality** per node (an importance signal
  stored on each node) and **concept co-occurrence** links — two concepts get a `relates_to` edge
  (weighted by shared-episode count) when they have events in the same episode.
- **Drift** (no LLM; needs the `[embeddings]` extra): buckets each concept's member events into
  time windows and scores how the concept's prototype (centroid) **moved** between windows; surfaced
  via `mem drift`. See the `mem drift` bullet above.
- **Distill** (needs the `[embeddings]` extra; LLM summary optional): derives a compact,
  retrieval-preserving stand-in for each L2/L3 node — a short summary (+ its embedding) and a
  **minimal subset of the node's real L0 evidence chunks** (the most representative ones, sized to
  `CMRAG_DISTILL_COMPRESSION_RATIO`). This is a research/measurement feature: it does **not** change
  `mem ask` ranking. Whether a distilled stand-in is good enough to adopt is decided by
  `scripts/distill_gate.py` (below), not by default. Provenance is preserved — the kept chunks are a
  real subset, never a generated paraphrase.

Every higher-level node drills down to its L0 evidence through its members.

```bash
mem build-memory --limit 50          # L1 events (up to 50 sources) + L2 episodes + L3 concepts + graph + drift
mem build-memory --level event       # only L1 events (LLM)
mem build-memory --level episode     # only L2 episodes (no Ollama needed)
mem build-memory --level concept     # only L3 concepts (needs the embeddings extra; run reindex-embeddings first)
mem build-memory --level graph       # only centrality + concept co-occurrence (no LLM/embeddings)
mem build-memory --level drift       # only concept-drift snapshots (needs the embeddings extra; build concepts first)
mem build-memory --level distill     # only distilled node stand-ins (needs the embeddings extra; build concepts first)
mem memory-stats                     # node/edge counts, co-occurrence edges, top central nodes, integrity
```

**Distillation gate (research/measurement).** `scripts/distill_gate.py` measures whether the
distilled stand-ins preserve retrieval quality against the full nodes, under a pre-committed budget
(`CMRAG_DISTILL_EPSILON`, default 0.05 max Recall@K loss; `CMRAG_DISTILL_COMPRESSION_RATIO`, default
0.5 target footprint). It prints `GATE: FIRE` only when recall is preserved within ε **and** the size
target is met. Run it on an embedded corpus after building concepts + distilling:

```bash
mem build-memory --db-path <db>                       # events/episodes/concepts (Ollama)
CMRAG_DB_PATH=<db> mem reindex-embeddings             # needs the [embeddings] extra
CMRAG_DB_PATH=<db> mem build-memory --level distill   # derive the distilled stand-ins
python scripts/distill_gate.py --db-path <db>         # full vs distilled Recall@K, ratio, FIRE/HOLD
```

Adopting distillation in the live retrieval path is a separate, explicitly-gated decision — the gate
must fire on a representative corpus first.

**Date-aware note dates.** A note's timestamp drives time-aware layers (episodes, drift). If a note
declares an explicit date — YAML frontmatter `date:`/`created:`, or a leading `Date: YYYY-MM-DD` line
near the top — that date is used as its timestamp; otherwise the file's modification time is used.
This keeps windowing deterministic across machines and checkouts.

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
  queries (incl. one negative/abstain case), `[sample-synth]` multi-source synthesis queries, `[sample-xmodal-text]` / `[sample-xmodal-visual]` cross-modal slices, and a `[sample-drift]` slice — a deliberately drifting "chunking" concept
  (two notes whose approach shifts across time windows) plus a stable provenance control
- Materializes tiny synthetic cross-modal fixtures (a 1-page PDF + two PNGs) under the sample vault.
  The PDF is ingested when the `[pdf]` extra is present and the images when `[ocr]` (+ tesseract) is
  present; otherwise those slices stay at a ~0 baseline. The two slices are designed so an
  OCR/PDF-text-first strategy answers the *text-heavy* slice but fails the *visual-heavy* one (a
  layout/colour-only diagram) — the gap the pre-committed native-embedding gate measures. Read the
  gate **semantically** (it compares against semantic image embeddings):
  `mem reindex-embeddings` first, then `python scripts/xmodal_gate.py` (it warns if no vectors are
  stored). Regenerate the fixtures with `scripts/generate_xmodal_fixtures.py`.
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

Distillation/drift scaffolding (additive, opt-in; defaults shown):

```bash
export CMRAG_DRIFT_WINDOW_DAYS=30          # concept-drift snapshot window length (days)
export CMRAG_DISTILL_EPSILON=0.05          # max Recall@K a distilled rep may lose vs full nodes (gate)
export CMRAG_DISTILL_COMPRESSION_RATIO=0.5 # target distilled-size / full-size for the distillation gate
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