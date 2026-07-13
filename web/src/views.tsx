import { useEffect, useState, type ReactNode } from "react";
import { motion } from "motion/react";
import {
  api,
  type DriftSummary,
  type ForgettingItem,
  type Health,
  type RecallCard,
} from "./api";

export interface ViewProps {
  health: Health | null;
}

/* ---------------- shared helpers ---------------- */

interface AsyncState<T> {
  data?: T;
  error?: string;
  loading: boolean;
}

function useAsync<T>(fn: () => Promise<T>, deps: unknown[]): AsyncState<T> {
  const [state, setState] = useState<AsyncState<T>>({ loading: true });
  useEffect(() => {
    let alive = true;
    setState({ loading: true });
    fn()
      .then((d) => alive && setState({ data: d, loading: false }))
      .catch((e) => alive && setState({ error: String(e?.message ?? e), loading: false }));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return state;
}

function ViewHead({
  eyebrow,
  title,
  lede,
  right,
}: {
  eyebrow: string;
  title: ReactNode;
  lede?: string;
  right?: ReactNode;
}) {
  return (
    <header className="view-head">
      <div className="spread">
        <div>
          <div className="eyebrow">{eyebrow}</div>
          <h1 className="view-title">{title}</h1>
        </div>
        {right}
      </div>
      {lede && <p className="view-lede text-pretty">{lede}</p>}
    </header>
  );
}

function Loading({ label = "Loading" }: { label?: string }) {
  return (
    <p className="loading" aria-live="polite">
      {label}…
    </p>
  );
}

function ErrorMsg({ error }: { error: string }) {
  return (
    <p className="error" role="alert">
      {error}
    </p>
  );
}

function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <h3>{title}</h3>
      <div className="muted">{children}</div>
    </div>
  );
}

const Stagger = ({ i, children }: { i: number; children: ReactNode }) => (
  <motion.div
    initial={{ opacity: 0, y: 8 }}
    animate={{ opacity: 1, y: 0 }}
    transition={{ duration: 0.34, delay: Math.min(i * 0.035, 0.4), ease: [0.2, 0.7, 0.2, 1] }}
  >
    {children}
  </motion.div>
);

function shortDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.slice(0, 10);
  return new Intl.DateTimeFormat(undefined, { year: "numeric", month: "short", day: "numeric" }).format(d);
}

function fileName(uri: string): string {
  return uri.split(/[\\/]/).pop() || uri;
}

/* ---------------- Concepts ---------------- */

export function ConceptsView() {
  const { data, error, loading } = useAsync(() => api.concepts(48), []);
  const items = data?.concepts ?? [];
  const max = Math.max(0.0001, ...items.map((c) => c.centrality));
  return (
    <>
      <ViewHead
        eyebrow="L3 · recurring abstractions"
        title={<>Concepts</>}
        lede="Durable topics clustered from your events, ranked by graph centrality — the substrate everything else drills back down from."
        right={data && <span className="count-pill num">{items.length}</span>}
      />
      {loading && <Loading label="Reading concepts" />}
      {error && <ErrorMsg error={error} />}
      {data && items.length === 0 && (
        <Empty title="No concepts yet">
          Build the hierarchy first: <code>mem build-memory</code> (needs the embeddings extra).
        </Empty>
      )}
      <div className="grid cols">
        {items.map((c, i) => (
          <Stagger i={i} key={c.node_id}>
            <article className="card">
              <div className="card-kicker" translate="no">
                concept #{c.node_id}
              </div>
              <h2 className="card-title">{c.title || "Untitled concept"}</h2>
              <div className="meter">
                <i style={{ width: `${(c.centrality / max) * 100}%` }} />
              </div>
              <div className="card-row">
                <span>
                  centrality <span className="num">{c.centrality.toFixed(3)}</span>
                </span>
                <span className="num">{c.members} events</span>
              </div>
            </article>
          </Stagger>
        ))}
      </div>
    </>
  );
}

/* ---------------- Timeline ---------------- */

export function TimelineView() {
  const { data, error, loading } = useAsync(() => api.timeline(120), []);
  // Newest first: the API returns episodes in ascending time order.
  const items = [...(data?.timeline ?? [])].sort((a, b) =>
    (b.time_start ?? "").localeCompare(a.time_start ?? ""),
  );
  return (
    <>
      <ViewHead
        eyebrow="L2 · sessions of work"
        title={
          <>
            The <em>timeline</em>
          </>
        }
        lede="Episodes group related events by project and time — a record of what you were doing then."
        right={data && <span className="count-pill num">{items.length}</span>}
      />
      {loading && <Loading label="Reading timeline" />}
      {error && <ErrorMsg error={error} />}
      {data && items.length === 0 && (
        <Empty title="No episodes yet">
          Run <code>mem build-memory</code> to derive episodes from your events.
        </Empty>
      )}
      <div className="timeline">
        {items.map((ep, i) => {
          const span =
            shortDate(ep.time_start) === shortDate(ep.time_end)
              ? shortDate(ep.time_start)
              : `${shortDate(ep.time_start)} → ${shortDate(ep.time_end)}`;
          return (
            <Stagger i={i} key={ep.node_id}>
              <div className="tl-item">
                <div className="tl-date mono">{span}</div>
                <div className="tl-title">{ep.title || "Untitled episode"}</div>
                <div className="tl-meta">
                  <span className="num">{ep.members}</span> events · episode #{ep.node_id}
                </div>
              </div>
            </Stagger>
          );
        })}
      </div>
    </>
  );
}

/* ---------------- Drift ---------------- */

export function DriftView() {
  const { data, error, loading } = useAsync(() => api.drift(48), []);
  const items = data?.drift ?? [];
  return (
    <>
      <ViewHead
        eyebrow="Concept movement over time"
        title={
          <>
            How a concept <em>drifted</em>
          </>
        }
        lede="Each concept's prototype is tracked across time windows; the sparkline traces how far it moved between consecutive windows."
        right={data && <span className="count-pill num">{items.length}</span>}
      />
      {loading && <Loading label="Reading drift snapshots" />}
      {error && <ErrorMsg error={error} />}
      {data && items.length === 0 && (
        <Empty title="No drift snapshots yet">
          Build them with <code>mem build-memory --level drift</code> (needs the embeddings extra).
        </Empty>
      )}
      <div>
        {items.map((d, i) => (
          <Stagger i={i} key={d.concept_id}>
            <DriftRow d={d} />
          </Stagger>
        ))}
      </div>
    </>
  );
}

function DriftRow({ d }: { d: DriftSummary }) {
  return (
    <div className="drift-row">
      <div style={{ minWidth: 0 }}>
        <div className="tl-title" style={{ fontSize: 17 }}>
          {d.title || "Untitled concept"}
          {d.relearning && (
            <span className="tag mod" style={{ marginLeft: 10, verticalAlign: "middle" }}>
              relearning
            </span>
          )}
        </div>
        <div className="tl-meta mono">
          {shortDate(d.span_start)} → {shortDate(d.span_end)} · {d.window_count} windows · support{" "}
          <span className="num">{d.support}</span>
        </div>
      </div>
      <Spark windows={d.windows} />
      <div className="drift-val">
        drift <b>{d.overall_drift.toFixed(3)}</b>
        <div className="faint" style={{ fontSize: 11 }}>
          conf {d.confidence.toFixed(2)}
        </div>
      </div>
    </div>
  );
}

function Spark({ windows }: { windows: DriftSummary["windows"] }) {
  const vals = windows.map((w) => w.drift_metric);
  if (vals.length < 2) {
    return <div className="faint mono" style={{ fontSize: 11 }}>single window</div>;
  }
  const w = 150;
  const h = 38;
  const max = Math.max(0.001, ...vals);
  const step = w / (vals.length - 1);
  const pts = vals.map((v, i) => [i * step, h - 4 - (v / max) * (h - 8)] as const);
  const dPath = pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
  const [lx, ly] = pts[pts.length - 1];
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${h}`} role="img" aria-label="Drift trajectory across windows">
      <path d={dPath} />
      <circle cx={lx} cy={ly} r={2.6} />
    </svg>
  );
}

/* ---------------- Forgetting + Recall ---------------- */

export function ForgettingView() {
  const { data, error, loading } = useAsync(() => api.forgetting("all", 24), []);
  const items = data?.forgetting ?? [];
  const [cards, setCards] = useState<RecallCard[] | null>(null);
  const [cardsLoading, setCardsLoading] = useState(false);
  const [cardsErr, setCardsErr] = useState<string | null>(null);

  async function loadCards() {
    setCardsLoading(true);
    setCardsErr(null);
    try {
      setCards((await api.recall("all", 8)).recall);
    } catch (e) {
      setCardsErr(String((e as Error)?.message ?? e));
    } finally {
      setCardsLoading(false);
    }
  }

  return (
    <>
      <ViewHead
        eyebrow="Importance × staleness"
        title={
          <>
            Likely <em>forgetting</em>
          </>
        }
        lede="Important memories you haven't revisited lately — each grounded to its evidence. Rehearsing one lowers its risk."
        right={data && <span className="count-pill num">{items.length}</span>}
      />
      {loading && <Loading label="Scoring memories" />}
      {error && <ErrorMsg error={error} />}
      {data && items.length === 0 && (
        <Empty title="Nothing at risk">
          Build memory and record some usage first: <code>mem build-memory</code>.
        </Empty>
      )}

      <div className="grid" style={{ gap: 0 }}>
        {items.map((f, i) => (
          <Stagger i={i} key={`${f.level}-${f.node_id}`}>
            <ForgettingRow f={f} />
          </Stagger>
        ))}
      </div>

      {items.length > 0 && (
        <>
          <hr className="section-rule" />
          <div className="spread" style={{ marginBottom: 16 }}>
            <h2 className="view-title" style={{ fontSize: 26 }}>
              Study cards
            </h2>
            <button className="btn" type="button" onClick={loadCards} disabled={cardsLoading}>
              {cardsLoading ? "Generating…" : cards ? "Regenerate" : "Generate study cards"}
            </button>
          </div>
          {cardsErr && <ErrorMsg error={cardsErr} />}
          {cards && cards.length === 0 && <p className="faint">No grounded cards available.</p>}
          <div className="grid cols">
            {cards?.map((c, i) => (
              <Stagger i={i} key={`${c.level}-${c.node_id}`}>
                <RecallCardView card={c} />
              </Stagger>
            ))}
          </div>
        </>
      )}
    </>
  );
}

function ForgettingRow({ f }: { f: ForgettingItem }) {
  return (
    <div className="evidence" style={{ paddingRight: 0 }}>
      <div className="ev-top">
        <span className="ev-id" translate="no">
          {f.risk.toFixed(2)}
        </span>
        <span className="ev-title">
          {f.title || "Untitled"} <span className="faint mono" style={{ fontSize: 11 }}>· {f.node_type}</span>
        </span>
      </div>
      <div className="ev-scores" style={{ marginTop: 12 }}>
        <Bar k="importance" v={f.importance} />
        <Bar k="staleness" v={f.staleness} cls="ox" />
        <Bar k="confidence" v={f.confidence} cls="sage" />
      </div>
      {f.evidence_source_uris.length > 0 && (
        <div className="ev-locator mono" style={{ marginTop: 10 }} translate="no">
          {f.evidence_source_uris.map(fileName).join(" · ")}
        </div>
      )}
    </div>
  );
}

function Bar({ k, v, cls = "" }: { k: string; v: number; cls?: string }) {
  return (
    <div className="score">
      <div className="score-k">
        {k} <span className="num">{v.toFixed(2)}</span>
      </div>
      <div className={`meter ${cls}`} style={{ marginTop: 4 }}>
        <i style={{ width: `${Math.max(0, Math.min(1, v)) * 100}%` }} />
      </div>
    </div>
  );
}

function RecallCardView({ card }: { card: RecallCard }) {
  const [shown, setShown] = useState(false);
  return (
    <article className="recall-card">
      <div className="card-kicker">
        <span className="num">risk {card.risk.toFixed(2)}</span> ·{" "}
        <span translate="no">{card.generated_by}</span>
      </div>
      <p className="recall-q" style={{ marginTop: 8 }}>
        {card.question}
      </p>
      {shown ? (
        <p className="recall-a">{card.answer || "—"}</p>
      ) : (
        <button className="reveal-btn" type="button" onClick={() => setShown(true)}>
          Reveal answer
        </button>
      )}
    </article>
  );
}

/* ---------------- Status ---------------- */

export function StatusView({ health }: ViewProps) {
  const live = useAsync(() => api.memoryStats(), []);
  const stats = live.data ?? health?.memory ?? null;
  return (
    <>
      <ViewHead
        eyebrow="Engine diagnostics"
        title={
          <>
            <em>Status</em> &amp; integrity
          </>
        }
        lede="A read-only snapshot of the local engine — what's installed, what's reachable, and how the memory graph is holding together."
      />

      {health && (
        <dl className="kv" style={{ marginBottom: 28 }}>
          <dt>database</dt>
          <dd translate="no">{health.db.path}</dd>
          <dt>llm (ollama)</dt>
          <dd className={health.ollama.reachable ? "on" : "off"}>
            {health.ollama.reachable ? `reachable · ${health.models.llm}` : "unreachable"}
          </dd>
          <dt>embeddings</dt>
          <dd className={health.extras.embeddings ? "on" : "off"}>
            {health.extras.embeddings ? (health.models.embed ?? "installed") : "lexical fallback"}
          </dd>
          <dt>pdf / ocr</dt>
          <dd>
            <span className={health.extras.pdf ? "on" : "off"}>pdf {health.extras.pdf ? "on" : "off"}</span>{" "}
            <span className={health.extras.ocr ? "on" : "off"}>· ocr {health.extras.ocr ? "on" : "off"}</span>
          </dd>
          <dt>config file</dt>
          <dd>{health.config.loaded ? (health.config.path ?? "loaded") : "none"}</dd>
        </dl>
      )}

      {live.loading && !stats && <Loading label="Reading memory stats" />}
      {stats && (
        <>
          <div className="stat-grid">
            <StatBox v={stats.total_nodes} k="memory nodes" />
            <StatBox v={stats.node_embeddings} k="node embeddings" />
            <StatBox v={stats.distilled_nodes} k="distilled" />
            <StatBox v={stats.drift_snapshots} k="drift snapshots" />
            <StatBox
              v={stats.integrity.unsupported_count}
              k="unsupported"
              tone={stats.integrity.unsupported_count ? "warn" : "ok"}
            />
            <StatBox
              v={stats.integrity.dangling_count}
              k="dangling edges"
              tone={stats.integrity.dangling_count ? "warn" : "ok"}
            />
          </div>
          {stats.top_central.length > 0 && (
            <>
              <hr className="section-rule" />
              <div className="eyebrow" style={{ marginBottom: 12 }}>
                Most central nodes
              </div>
              <div className="grid" style={{ gap: 0 }}>
                {stats.top_central.map((n) => (
                  <div className="card-row" key={n.node_id} style={{ borderBottom: "1px solid var(--rule)", padding: "10px 2px" }}>
                    <span>
                      <span className="faint mono">L{n.level}</span> {n.title || "untitled"}
                    </span>
                    <span className="num">{n.centrality.toFixed(3)}</span>
                  </div>
                ))}
              </div>
            </>
          )}
        </>
      )}
    </>
  );
}

function StatBox({ v, k, tone }: { v: number; k: string; tone?: "warn" | "ok" }) {
  return (
    <div className="stat">
      <div className={`stat-v ${tone ?? ""}`}>{v}</div>
      <div className="stat-k">{k}</div>
    </div>
  );
}
