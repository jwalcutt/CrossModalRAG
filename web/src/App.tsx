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

function NavIcon({ name }: { name: string }) {
  const paths: Record<string, JSX.Element> = {
    concepts: (
      <>
        <circle cx="18" cy="5" r="3" />
        <circle cx="6" cy="12" r="3" />
        <circle cx="18" cy="19" r="3" />
        <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
        <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
      </>
    ),
    timeline: (
      <>
        <circle cx="12" cy="12" r="9" />
        <polyline points="12 7 12 12 15.5 14" />
      </>
    ),
    drift: (
      <>
        <path d="M2 6c.6.5 1.2 1 2.5 1C7 7 7 5 9.5 5c2.6 0 2.4 2 5 2 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1" />
        <path d="M2 12c.6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 2.6 0 2.4 2 5 2 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1" />
        <path d="M2 18c.6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 2.6 0 2.4 2 5 2 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1" />
      </>
    ),
    forgetting: (
      <>
        <path d="M5 22h14" />
        <path d="M5 2h14" />
        <path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22" />
        <path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2" />
      </>
    ),
    status: <path d="M22 12h-4l-3 9L9 3l-3 9H2" />,
    settings: (
      <>
        <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
        <circle cx="12" cy="12" r="3" />
      </>
    ),
  };
  return (
    <svg
      className="nav-icon"
      viewBox="0 0 24 24"
      width="16"
      height="16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {paths[name]}
    </svg>
  );
}

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
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [collapsed, setCollapsed] = useState<boolean>(
    () => localStorage.getItem("rail-collapsed") === "1",
  );
  const [convsCollapsed, setConvsCollapsed] = useState<boolean>(
    () => localStorage.getItem("convs-collapsed") === "1",
  );

  useEffect(() => {
    localStorage.setItem("rail-collapsed", collapsed ? "1" : "0");
  }, [collapsed]);

  useEffect(() => {
    localStorage.setItem("convs-collapsed", convsCollapsed ? "1" : "0");
  }, [convsCollapsed]);

  useEffect(() => {
    let alive = true;
    api
      .health()
      .then((h) => alive && setHealth(h))
      .catch(() => undefined); // views degrade gracefully without health info
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

  const isChat = route.view === "chat";
  const viewKey = isChat ? `chat:${route.conversationId ?? "new"}` : route.view;

  return (
    <MotionConfig reducedMotion="user">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <div className={`app${collapsed ? " collapsed" : ""}`}>
        <aside className="rail">
          <div className="brand">
            <div className="brand-row">
              <div className="brand-mark" translate="no">
                CMRAG
              </div>
              <button
                type="button"
                className="rail-toggle"
                aria-expanded={!collapsed}
                aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
                title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
                onClick={() => setCollapsed((c) => !c)}
              >
                <svg
                  viewBox="0 0 24 24"
                  width="17"
                  height="17"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  aria-hidden="true"
                >
                  <rect x="3" y="3" width="18" height="18" rx="2" />
                  <line x1="9" y1="3" x2="9" y2="21" />
                </svg>
              </button>
            </div>
            <div className="brand-name">
              Memory <em>Console</em>
            </div>
          </div>

          <nav className="nav" aria-label="Sections">
            <button
              type="button"
              className="nav-item"
              aria-current={isChat && route.conversationId === null ? "page" : undefined}
              title={collapsed ? "New chat" : undefined}
              onClick={() => navigate("chat")}
            >
              <svg
                className="nav-icon"
                viewBox="0 0 24 24"
                width="16"
                height="16"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.8"
                strokeLinecap="round"
                strokeLinejoin="round"
                aria-hidden="true"
              >
                <line x1="12" y1="5" x2="12" y2="19" />
                <line x1="5" y1="12" x2="19" y2="12" />
              </svg>
              <span className="nav-label">New chat</span>
            </button>
            {NAV.map((n) => (
              <button
                key={n.key}
                type="button"
                className="nav-item"
                aria-current={route.view === n.key ? "page" : undefined}
                title={collapsed ? n.label : undefined}
                onClick={() => navigate(n.key)}
              >
                <NavIcon name={n.key} />
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
              <NavIcon name="settings" />
              <span className="nav-label">Settings</span>
            </button>
          </nav>

          {conversations.length > 0 && (
            <nav
              className={`conv-panel${convsCollapsed ? " conv-panel--collapsed" : ""}`}
              aria-label="Conversations"
            >
              <div className="conv-panel-head nav-label">
                <span>Conversations</span>
                <button
                  type="button"
                  className="conv-toggle"
                  aria-expanded={!convsCollapsed}
                  aria-label={convsCollapsed ? "Expand conversations" : "Collapse conversations"}
                  title={convsCollapsed ? "Expand conversations" : "Collapse conversations"}
                  onClick={() => setConvsCollapsed((c) => !c)}
                >
                  <svg
                    viewBox="0 0 24 24"
                    width="14"
                    height="14"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    aria-hidden="true"
                  >
                    <polyline points="6 9 12 15 18 9" />
                  </svg>
                </button>
              </div>
              {!convsCollapsed && (
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
              )}
            </nav>
          )}
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
