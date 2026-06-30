import { useEffect, useState } from "react";
import { AnimatePresence, motion, MotionConfig } from "motion/react";
import { api, type Health } from "./api";
import {
  AskView,
  ConceptsView,
  DriftView,
  ForgettingView,
  StatusView,
  TimelineView,
  type ViewProps,
} from "./views";

const NAV: { key: string; label: string; el: (p: ViewProps) => JSX.Element }[] = [
  { key: "ask", label: "Ask", el: AskView },
  { key: "concepts", label: "Concepts", el: ConceptsView },
  { key: "timeline", label: "Timeline", el: TimelineView },
  { key: "drift", label: "Drift", el: DriftView },
  { key: "forgetting", label: "Forgetting", el: ForgettingView },
  { key: "status", label: "Status", el: StatusView },
];

function currentHash(): string {
  return window.location.hash.replace("#", "") || "ask";
}

export function App() {
  const [view, setView] = useState<string>(currentHash);
  const [health, setHealth] = useState<Health | null>(null);
  const [healthErr, setHealthErr] = useState(false);

  useEffect(() => {
    let alive = true;
    api
      .health()
      .then((h) => alive && setHealth(h))
      .catch(() => alive && setHealthErr(true));
    return () => {
      alive = false;
    };
  }, []);

  // URL reflects the active section (deep-linkable, back/forward works).
  useEffect(() => {
    if (currentHash() !== view) window.location.hash = view;
  }, [view]);
  useEffect(() => {
    const onHash = () => setView(currentHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const Active = (NAV.find((n) => n.key === view) ?? NAV[0]).el;
  const dotClass = healthErr ? "warn" : health ? (health.db.exists ? "live" : "idle") : "idle";
  const statusText = healthErr
    ? "api offline"
    : health
      ? health.db.exists
        ? "engine ready"
        : "no database"
      : "connecting…";

  return (
    <MotionConfig reducedMotion="user">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <div className="app">
        <aside className="rail">
          <div className="brand">
            <div className="brand-mark" translate="no">
              CMRAG
            </div>
            <div className="brand-name">
              Memory <em>console</em>
            </div>
            <div className="brand-sub">evidence-grounded · local-first</div>
          </div>

          <nav className="nav" aria-label="Sections">
            {NAV.map((n, i) => (
              <button
                key={n.key}
                type="button"
                className="nav-item"
                aria-current={view === n.key ? "page" : undefined}
                onClick={() => setView(n.key)}
              >
                <span className="nav-index num" aria-hidden="true">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <span className="nav-label">{n.label}</span>
              </button>
            ))}
          </nav>

          <div className="rail-foot">
            <div className="status-line" aria-live="polite">
              <span className={`dot ${dotClass}`} aria-hidden="true" />
              <span className="status-text">{statusText}</span>
            </div>
            {health && (
              <div className="status-meta">
                <div>
                  <b>llm</b> {health.ollama.reachable ? health.models.llm : "offline"}
                </div>
                <div>
                  <b>embed</b> {health.extras.embeddings ? (health.models.embed ?? "on") : "lexical"}
                </div>
                <div>
                  <b>nodes</b> <span className="num">{health.memory?.total_nodes ?? 0}</span>
                </div>
              </div>
            )}
          </div>
        </aside>

        <main className="main" id="main">
          <AnimatePresence mode="wait">
            <motion.div
              key={view}
              className="view"
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.3, ease: [0.2, 0.7, 0.2, 1] }}
            >
              <Active health={health} />
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </MotionConfig>
  );
}
