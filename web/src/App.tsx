import { useCallback, useEffect, useState } from "react";
import { AnimatePresence, motion, MotionConfig } from "motion/react";
import { api, type ConversationSummary, type Health } from "./api";
import { ChatView } from "./chat";
import { SettingsView } from "./settings-view";
import {
  ConceptsView,
  DriftView,
  ForgettingView,
  StatusView,
  TimelineView,
  type ViewProps,
} from "./views";

const NAV: { key: string; label: string; el: (p: ViewProps) => JSX.Element }[] = [
  { key: "concepts", label: "Concepts", el: ConceptsView },
  { key: "timeline", label: "Timeline", el: TimelineView },
  { key: "drift", label: "Drift", el: DriftView },
  { key: "forgetting", label: "Forgetting", el: ForgettingView },
  { key: "status", label: "Status", el: StatusView },
];

/** Route model: `#chat` (fresh chat), `#c/<id>` (a saved conversation),
 *  `#settings`, or a section key from NAV. Unknown/empty hashes land on chat. */
interface Route {
  view: string;
  conversationId: number | null;
}

function parseHash(): Route {
  const hash = window.location.hash.replace(/^#/, "");
  const convMatch = /^c\/(\d+)$/.exec(hash);
  if (convMatch) return { view: "chat", conversationId: Number(convMatch[1]) };
  if (hash === "" || hash === "chat" || hash === "ask") return { view: "chat", conversationId: null };
  if (hash === "settings" || NAV.some((n) => n.key === hash)) {
    return { view: hash, conversationId: null };
  }
  return { view: "chat", conversationId: null };
}

function shortWhen(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  return sameDay
    ? new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(d)
    : new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" }).format(d);
}

export function App() {
  const [route, setRoute] = useState<Route>(parseHash);
  const [health, setHealth] = useState<Health | null>(null);
  const [healthErr, setHealthErr] = useState(false);
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [collapsed, setCollapsed] = useState<boolean>(
    () => localStorage.getItem("rail-collapsed") === "1",
  );

  useEffect(() => {
    localStorage.setItem("rail-collapsed", collapsed ? "1" : "0");
  }, [collapsed]);

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

  const refreshConversations = useCallback(() => {
    api
      .conversations(30)
      .then((r) => setConversations(r.conversations))
      .catch(() => undefined); // sidebar history is best-effort
  }, []);

  useEffect(() => {
    refreshConversations();
  }, [refreshConversations]);

  useEffect(() => {
    const onHash = () => setRoute(parseHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const navigate = (hash: string) => {
    window.location.hash = hash;
  };

  // A chat turn created or touched a conversation: refresh the panel so titles
  // (LLM-generated after the first answer) and ordering stay current.
  const onConversationUpdate = useCallback(
    (_conv: ConversationSummary | null) => {
      refreshConversations();
    },
    [refreshConversations],
  );

  const dotClass = healthErr ? "warn" : health ? (health.db.exists ? "live" : "idle") : "idle";
  const statusText = healthErr
    ? "api offline"
    : health
      ? health.db.exists
        ? "engine ready"
        : "no database"
      : "connecting…";

  const isChat = route.view === "chat";
  const viewKey = isChat ? `chat:${route.conversationId ?? "new"}` : route.view;

  return (
    <MotionConfig reducedMotion="user">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <div className={`app${collapsed ? " collapsed" : ""}`}>
        <aside className="rail">
          <button
            type="button"
            className="rail-toggle"
            aria-expanded={!collapsed}
            aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
            onClick={() => setCollapsed((c) => !c)}
          >
            <span aria-hidden="true">{collapsed ? "»" : "«"}</span>
          </button>

          <div className="brand">
            <div className="brand-mark" translate="no">
              CMRAG
            </div>
            <div className="brand-name">
              Memory <em>console</em>
            </div>
            <div className="brand-sub">evidence-grounded · local-first</div>
          </div>

          <button
            type="button"
            className="new-chat-btn"
            onClick={() => navigate("chat")}
            title={collapsed ? "New chat" : undefined}
          >
            <span aria-hidden="true">＋</span>
            <span className="nav-label">New chat</span>
          </button>

          {conversations.length > 0 && (
            <nav className="conv-panel" aria-label="Conversations">
              <div className="conv-panel-head nav-label">Conversations</div>
              <div className="conv-list">
                {conversations.map((c) => (
                  <button
                    key={c.id}
                    type="button"
                    className="conv-item"
                    aria-current={
                      isChat && route.conversationId === c.id ? "page" : undefined
                    }
                    title={collapsed ? (c.title ?? `#${c.id}`) : undefined}
                    onClick={() => navigate(`c/${c.id}`)}
                  >
                    <span className="conv-title nav-label">{c.title || `Conversation #${c.id}`}</span>
                    <span className="conv-when num nav-label">{shortWhen(c.updated_at)}</span>
                  </button>
                ))}
              </div>
            </nav>
          )}

          <nav className="nav" aria-label="Sections">
            {NAV.map((n, i) => (
              <button
                key={n.key}
                type="button"
                className="nav-item"
                aria-current={route.view === n.key ? "page" : undefined}
                title={collapsed ? n.label : undefined}
                onClick={() => navigate(n.key)}
              >
                <span className="nav-index num" aria-hidden="true">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <span className="nav-label">{n.label}</span>
              </button>
            ))}
            <button
              type="button"
              className="nav-item"
              aria-current={route.view === "settings" ? "page" : undefined}
              title={collapsed ? "Settings" : undefined}
              onClick={() => navigate("settings")}
            >
              <span className="nav-index num" aria-hidden="true">
                {String(NAV.length + 1).padStart(2, "0")}
              </span>
              <span className="nav-label">Settings</span>
            </button>
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
              key={viewKey}
              className={`view${isChat ? " view--chat" : ""}`}
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -8 }}
              transition={{ duration: 0.3, ease: [0.2, 0.7, 0.2, 1] }}
            >
              {isChat ? (
                <ChatView
                  conversationId={route.conversationId}
                  health={health}
                  onConversationUpdate={onConversationUpdate}
                />
              ) : route.view === "settings" ? (
                <SettingsView health={health} />
              ) : (
                (() => {
                  const Active = (NAV.find((n) => n.key === route.view) ?? NAV[0]).el;
                  return <Active health={health} />;
                })()
              )}
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </MotionConfig>
  );
}
