# CrossModalRAG web console

The local web UI for the memory engine — a React + Vite app that consumes the read-only API exposed by
`mem serve`. It is a **thin client**: it only renders the JSON contracts (`/ask`, `/concepts`,
`/timeline`, `/drift`, `/forgetting`, `/recall`, `/memory-stats`, `/health`) and adds no retrieval or
derivation logic of its own. Everything is local-first — assets and fonts are vendored, no external
calls at runtime.

## Build (ships with the `[ui]` extra)

The build output is committed to `../src/crossmodalrag/api/static/` and served by FastAPI at `/`, so
**end users need no Node** — `pip install -e ".[ui]"` + `mem serve` is enough.

```bash
npm install
npm run build      # -> ../src/crossmodalrag/api/static/  (commit the result)
npm test           # vitest unit tests (markdown/citation rendering)
```

## Develop

```bash
# 1. run the API in one terminal:
mem serve                      # http://127.0.0.1:8765

# 2. run the Vite dev server in another (proxies /ask, /health, … to :8765):
npm run dev                    # http://localhost:5173
```

## Layout

- `src/api.ts` — typed client over the local endpoints (same-origin `fetch`).
- `src/App.tsx` — app shell: sidebar nav, engine status, hash-routed sections.
- `src/views.tsx` — the views (Ask, Concepts, Timeline, Drift, Forgetting/Recall, Status).
- `src/markdown.tsx` — markdown rendering for synthesized answers: `marked` → DOMPurify (strict
  tag/attr allowlist; LLM output is untrusted, raw HTML is stripped) → React, with `[E#]` citation
  tokens turned into clickable chips *after* sanitization (so chips can only come from our own
  transform, and a literal `[E#]` inside code stays code). Both libraries are vendored into the
  bundle — no CDN. Streaming re-renders the accumulated markdown per token.
- `src/styles.css` — the design system (an "archival instrument" theme: warm charcoal, ink, amber).
