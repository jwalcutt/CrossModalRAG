import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from "react";
import {
  api,
  type ChatMessagePayload,
  type ConversationSummary,
  type EvidenceItem,
  type Health,
} from "./api";
import { AnswerMarkdown } from "./markdown";
import { loadSettings } from "./settings";

/* The conversation thread: an archival correspondence — the user's questions as
   right-set marginalia, the engine's grounded replies as full-measure prose with
   their evidence folios tucked beneath. Composer pinned to the foot of the page. */

interface ThreadMessage {
  role: "user" | "assistant";
  text: string;
  evidence?: EvidenceItem[] | null;
  abstention_reason?: string | null;
  truncated?: boolean;
  model?: string | null;
}

function fileName(uri: string): string {
  return uri.split(/[\\/]/).pop() || uri;
}

function fromStored(m: ChatMessagePayload): ThreadMessage {
  return {
    role: m.role,
    text: m.text,
    evidence: m.evidence,
    abstention_reason: m.abstention_reason,
    truncated: m.truncated,
    model: m.model,
  };
}

export function ChatView({
  conversationId,
  health,
  onConversationUpdate,
}: {
  conversationId: number | null;
  health: Health | null;
  onConversationUpdate: (conv: ConversationSummary | null) => void;
}) {
  const [messages, setMessages] = useState<ThreadMessage[]>([]);
  const [title, setTitle] = useState<string | null>(null);
  const [liveText, setLiveText] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadingStored, setLoadingStored] = useState(false);
  const [saveDisabled, setSaveDisabled] = useState(false);

  // The id this view is currently showing; kept in a ref so the first turn can
  // adopt the freshly created conversation without a remount/refetch.
  const activeId = useRef<number | null>(null);
  const endRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);

  const llmReady = health?.ollama.reachable ?? false;

  // Route changes: load the stored conversation (or reset for a fresh chat).
  useEffect(() => {
    if (conversationId === activeId.current) return;
    activeId.current = conversationId;
    setError(null);
    setLiveText(null);
    if (conversationId === null) {
      setMessages([]);
      setTitle(null);
      return;
    }
    let alive = true;
    setLoadingStored(true);
    api
      .conversation(conversationId)
      .then((conv) => {
        if (!alive) return;
        setMessages(conv.messages.map(fromStored));
        setTitle(conv.title);
      })
      .catch((e) => alive && setError(String((e as Error)?.message ?? e)))
      .finally(() => alive && setLoadingStored(false));
    return () => {
      alive = false;
    };
  }, [conversationId]);

  const scrollToEnd = useCallback((behavior: ScrollBehavior = "smooth") => {
    endRef.current?.scrollIntoView({ behavior, block: "end" });
  }, []);

  useEffect(() => {
    scrollToEnd(messages.length > 0 && liveText === null ? "auto" : "smooth");
  }, [messages, liveText, scrollToEnd]);

  async function send(e?: FormEvent) {
    e?.preventDefault();
    const q = input.trim();
    if (!q || busy) return;
    const settings = loadSettings();
    setBusy(true);
    setError(null);
    setInput("");
    setMessages((m) => [...m, { role: "user", text: q }]);
    setLiveText("");
    try {
      const result = await api.chatStream(
        q,
        activeId.current,
        {
          profile: settings.profile,
          level: settings.level,
          top_k: settings.topK,
          use_llm: settings.synthesize && llmReady,
          save: settings.save,
        },
        (text) => setLiveText((t) => (t ?? "") + text),
      );
      const p = result.payload;
      setMessages((m) => [
        ...m,
        {
          role: "assistant",
          text: p.answer ?? "",
          evidence: p.evidence,
          abstention_reason: p.abstained ? "abstained" : null,
          truncated: p.truncated,
          model: p.model,
        },
      ]);
      setSaveDisabled(result.conversationId === null);
      if (result.conversationId !== null && activeId.current === null) {
        // First turn of a fresh chat: adopt the new conversation in place, then
        // reflect it in the URL + sidebar (no remount — activeId already matches).
        activeId.current = result.conversationId;
        setTitle(result.conversation?.title ?? null);
        onConversationUpdate(result.conversation);
        window.location.hash = `c/${result.conversationId}`;
      } else if (result.conversation) {
        onConversationUpdate(result.conversation);
      }
    } catch (err) {
      setError(String((err as Error)?.message ?? err));
      setMessages((m) => m.slice(0, -1)); // withdraw the unanswered question
      setInput(q); // let the user retry without retyping
    } finally {
      setBusy(false);
      setLiveText(null);
      inputRef.current?.focus();
    }
  }

  function onComposerKey(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      void send();
    }
  }

  const empty = messages.length === 0 && liveText === null && !loadingStored;

  return (
    <div className="chat-wrap">
      <header className="chat-head">
        <div className="eyebrow">{conversationId ? "Saved conversation" : "Grounded chat"}</div>
        <h1 className="chat-title text-pretty">
          {title ?? (
            <>
              Ask the <em>archive</em>
            </>
          )}
        </h1>
      </header>

      <div className="chat-thread" aria-live="polite">
        {loadingStored && <p className="loading">Opening conversation…</p>}
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        {empty && (
          <div className="chat-empty">
            <h2>A conversation with your own record</h2>
            <p className="muted text-pretty">
              Every reply is retrieved from and cited against your notes, commits, and documents —
              and the thread remembers itself. Follow-ups like “expand on that” just work.
            </p>
          </div>
        )}
        {messages.map((m, i) =>
          m.role === "user" ? (
            <UserMessage key={i} text={m.text} />
          ) : (
            <AssistantMessage key={i} message={m} />
          ),
        )}
        {liveText !== null && (
          <article className="msg msg-assistant">
            <div className="answer-meta">
              <span className="badge model">synthesizing…</span>
            </div>
            {liveText ? (
              <div className="answer-prose text-pretty">
                <AnswerMarkdown text={liveText} onCite={() => undefined} />
                <span className="stream-cursor" aria-hidden="true" />
              </div>
            ) : (
              <p className="muted">
                Retrieving evidence… <span className="stream-cursor" aria-hidden="true" />
              </p>
            )}
          </article>
        )}
        <div ref={endRef} />
      </div>

      <form className="chat-composer" onSubmit={send}>
        {saveDisabled && (
          <p className="composer-note">
            History saving is off — context will not carry between messages.
          </p>
        )}
        <div className="composer-row">
          <textarea
            ref={inputRef}
            className="composer-input"
            rows={1}
            value={input}
            placeholder={conversationId ? "Continue the conversation…" : "Ask your archive…"}
            aria-label="Message"
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={onComposerKey}
            // eslint-disable-next-line jsx-a11y/no-autofocus
            autoFocus
          />
          <button className="btn primary" type="submit" disabled={busy || !input.trim()}>
            {busy ? "…" : "Send"}
          </button>
        </div>
        <p className="composer-hint">
          Enter to send · Shift+Enter for a new line · citations always come from the current
          turn’s evidence
        </p>
      </form>
    </div>
  );
}

function UserMessage({ text }: { text: string }) {
  return (
    <article className="msg msg-user">
      <div className="msg-user-card">{text}</div>
    </article>
  );
}

function AssistantMessage({ message }: { message: ThreadMessage }) {
  const [ledgerOpen, setLedgerOpen] = useState(false);
  const [flash, setFlash] = useState<string | null>(null);
  const evidenceRefs = useRef<Record<string, HTMLElement | null>>({});
  const evidence = message.evidence ?? [];
  const abstained = Boolean(message.abstention_reason);

  function jumpToEvidence(id: string) {
    setLedgerOpen(true);
    // Defer the scroll until the ledger has rendered open.
    window.setTimeout(() => {
      const el = evidenceRefs.current[id];
      if (!el) return;
      el.scrollIntoView({ behavior: "smooth", block: "center" });
      setFlash(id);
      window.setTimeout(() => setFlash((f) => (f === id ? null : f)), 1100);
    }, 60);
  }

  return (
    <article className="msg msg-assistant">
      <div className="answer-meta">
        {message.model ? (
          <span className="badge model" translate="no">
            {message.model}
          </span>
        ) : (
          <span className="badge template">evidence template</span>
        )}
        {evidence.length > 0 && (
          <span className="count-pill num">
            {evidence.length} source{evidence.length === 1 ? "" : "s"}
          </span>
        )}
      </div>

      {abstained ? (
        <div className="abstain">
          <b>Insufficient evidence</b>
          {message.text ||
            "The retrieved evidence was too weak to answer confidently — so the engine declines rather than guess."}
        </div>
      ) : message.text ? (
        <div className="answer-prose text-pretty">
          <AnswerMarkdown text={message.text} onCite={jumpToEvidence} />
        </div>
      ) : message.truncated ? (
        <div className="abstain">
          <b>Answer cut off</b>
          The model's context window filled before any answer text was produced. Try a narrower
          question, or raise <code>CMRAG_LLM_NUM_CTX</code>.
        </div>
      ) : (
        <p className="muted">No synthesized answer — see the retrieved evidence below.</p>
      )}
      {message.truncated && message.text && !abstained && (
        <p className="truncation-note" role="status">
          ⚠ This answer was cut off mid-generation (context window filled) — it is incomplete,
          not final. Try a narrower question, or raise <code>CMRAG_LLM_NUM_CTX</code>.
        </p>
      )}

      {evidence.length > 0 && (
        <section className="msg-ledger" aria-label="Evidence ledger">
          <button
            type="button"
            className="ledger-toggle"
            aria-expanded={ledgerOpen}
            onClick={() => setLedgerOpen((o) => !o)}
          >
            <span className="ledger-caret" aria-hidden="true">
              {ledgerOpen ? "▾" : "▸"}
            </span>
            Evidence ledger
            <span className="num">{evidence.length}</span>
          </button>
          {ledgerOpen && (
            <div className="ledger-grid">
              {evidence.map((ev) => (
                <article
                  key={ev.evidence_id}
                  ref={(el) => {
                    evidenceRefs.current[ev.evidence_id] = el;
                  }}
                  className={`evidence ${ev.cited ? "cited" : ""} ${
                    flash === ev.evidence_id ? "flash" : ""
                  }`}
                >
                  <div className="ev-top">
                    <span className={`ev-id ${ev.cited ? "" : "dim"}`} translate="no">
                      {ev.evidence_id}
                    </span>
                    <span className="ev-title">{ev.title || fileName(ev.source_uri)}</span>
                  </div>
                  <div className="ev-locator mono" translate="no" title={ev.source_uri}>
                    {ev.locator}
                  </div>
                  <div className="ev-tags">
                    {ev.modality && <span className="tag mod">{ev.modality}</span>}
                    <span className="tag">{ev.source_type}</span>
                    {ev.ocr_confidence != null && (
                      <span className="tag">ocr {(ev.ocr_confidence * 100).toFixed(0)}%</span>
                    )}
                  </div>
                  {ev.excerpt && <p className="ev-excerpt">{ev.excerpt}</p>}
                  <div className="ev-scores">
                    {(["vector", "lexical", "recency"] as const).map((k) => (
                      <div className="score" key={k}>
                        <div className="score-k">{k}</div>
                        <div className="score-bar">
                          <i
                            style={{
                              width: `${Math.max(0, Math.min(1, ev.scores[k])) * 100}%`,
                            }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </article>
              ))}
            </div>
          )}
        </section>
      )}
    </article>
  );
}
