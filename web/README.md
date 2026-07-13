# CrossModalRAG web console

The local web UI for the memory engine — a React + Vite app over the local API exposed by
`mem serve`. It is a **thin client**: it only renders the library's JSON contracts and adds no
retrieval or derivation logic of its own. Everything is local-first — assets and fonts are
vendored, no external calls at runtime.

The primary surface is a **grounded chat**: multi-turn conversations with carried context, each
saved conversation at its own route (`#c/<id>`), navigable and resumable from the sidebar's
history panel. Every assistant reply keeps full provenance — markdown-rendered prose with `[E#]`
citation chips and a collapsible per-message evidence ledger (rendered from the stored
point-in-time snapshot, so drill-down survives re-chunking). The composer sits at the foot of the
page; retrieval/generation parameters live on the Settings page (localStorage-persisted, applied
per request).

Consumed endpoints: `/chat/stream` (POST) and `/conversations/{id}` (DELETE — the sidebar's
per-conversation delete, behind a two-step confirm) are the API's only write paths, both scoped
to the user-owned chat-history tables; plus the reads `/conversations`, `/conversations/{id}`,
`/concepts`, `/timeline`, `/drift`, `/forgetting`, `/recall`, `/memory-stats`, `/health`.

## Build (ships with the `[ui]` extra)

The build output is committed to `../src/crossmodalrag/api/static/` and served by FastAPI at `/`, so
**end users do not need Node** — `pip install -e ".[ui]"` + `mem serve` is enough.

```bash
npm install
npm run build      # -> ../src/crossmodalrag/api/static/  (commit the result)
npm test           # vitest unit tests (markdown/citation rendering)
```

## Develop

```bash
# 1. run the API in one terminal:
mem serve                      # http://127.0.0.1:8765

# 2. run the Vite dev server in another (proxies /chat, /health, … to :8765):
npm run dev                    # http://localhost:5173
```

## Layout

- `src/api.ts` — typed client over the local endpoints (same-origin `fetch`), including the
  NDJSON chat-stream reader.
- `src/App.tsx` — app shell: hash routing (`#chat`, `#c/<id>`, `#settings`, sections), sidebar
  with New chat + conversation history panel + section nav + engine status.
- `src/chat.tsx` — the conversation view: message thread (user marginalia right, grounded prose
  full-measure), streaming, per-message collapsible evidence ledger, bottom composer, resume on
  route load.
- `src/settings.ts` / `src/settings-view.tsx` — client-side retrieval/generation settings
  (profile, memory level, evidence per turn, synthesis, history saving), localStorage-persisted.
- `src/views.tsx` — the read views (Concepts, Timeline, Drift, Forgetting/Recall, Status).
- `src/markdown.tsx` — markdown rendering for synthesized answers: `marked` → DOMPurify (strict
  tag/attr allowlist; LLM output is untrusted, raw HTML is stripped) → React, with `[E#]` citation
  tokens turned into clickable chips *after* sanitization (so chips can only come from our own
  transform, and a literal `[E#]` inside code stays code). Both libraries are vendored into the
  bundle — no CDN. Streaming re-renders the accumulated markdown per token.
- `src/styles.css` — the design system (an "archival instrument" theme: warm charcoal, ink, amber).
