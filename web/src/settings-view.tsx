import { useEffect, useState } from "react";
import type { Health } from "./api";
import {
  DEFAULT_SETTINGS,
  loadSettings,
  saveSettings,
  type ChatSettings,
} from "./settings";

/* Model & retrieval settings, consolidated from the old per-query chips.
   Every knob here maps onto a per-request parameter of the local API and is
   applied to each chat turn; engine-level tuning stays in `.env`. */

const PROFILES: { key: ChatSettings["profile"]; blurb: string }[] = [
  { key: "balanced", blurb: "Even blend of meaning, wording, and recency." },
  { key: "relevant", blurb: "Semantic meaning first — the default for questions." },
  { key: "recent", blurb: "Recency-weighted — “what was I doing lately?”" },
  { key: "usage", blurb: "Boosts memories you actually revisit (needs usage tracking)." },
];

const LEVELS: { key: ChatSettings["level"]; blurb: string }[] = [
  { key: "evidence", blurb: "Raw chunks (L0) — precise, literal grounding." },
  { key: "event", blurb: "Enter at atomic events, drill down to evidence." },
  { key: "episode", blurb: "Enter at work sessions — good for “what happened”." },
  { key: "concept", blurb: "Enter at durable concepts — good for synthesis." },
];

export function SettingsView({ health }: { health: Health | null }) {
  const [settings, setSettings] = useState<ChatSettings>(loadSettings);
  const [savedFlash, setSavedFlash] = useState(false);

  useEffect(() => {
    saveSettings(settings);
    setSavedFlash(true);
    const t = window.setTimeout(() => setSavedFlash(false), 900);
    return () => window.clearTimeout(t);
  }, [settings]);

  function set<K extends keyof ChatSettings>(key: K, value: ChatSettings[K]) {
    setSettings((s) => ({ ...s, [key]: value }));
  }

  return (
    <>
      <header className="view-head">
        <div className="spread">
          <div>
            <div className="eyebrow">Per-request parameters</div>
            <h1 className="view-title">
              Model <em>settings</em>
            </h1>
          </div>
          <span className={`save-flash ${savedFlash ? "on" : ""}`} aria-live="polite">
            saved
          </span>
        </div>
        <p className="view-lede text-pretty">
          Applied to every chat turn, stored in this browser. Engine-level tuning
          (context window, abstention threshold, models) lives in <code>.env</code>.
        </p>
      </header>

      <section className="setting-card">
        <div className="setting-head">
          <h2>Retrieval profile</h2>
          <p className="muted">How evidence candidates are ranked before the answer is written.</p>
        </div>
        <div className="setting-options" role="radiogroup" aria-label="Retrieval profile">
          {PROFILES.map((p) => (
            <button
              key={p.key}
              type="button"
              role="radio"
              aria-checked={settings.profile === p.key}
              className={`setting-option ${settings.profile === p.key ? "active" : ""}`}
              onClick={() => set("profile", p.key)}
            >
              <span className="option-name mono">{p.key}</span>
              <span className="option-blurb">{p.blurb}</span>
            </button>
          ))}
        </div>
      </section>

      <section className="setting-card">
        <div className="setting-head">
          <h2>Memory level</h2>
          <p className="muted">
            Where retrieval enters the hierarchy; every level still grounds and cites raw evidence.
          </p>
        </div>
        <div className="setting-options" role="radiogroup" aria-label="Memory level">
          {LEVELS.map((l) => (
            <button
              key={l.key}
              type="button"
              role="radio"
              aria-checked={settings.level === l.key}
              className={`setting-option ${settings.level === l.key ? "active" : ""}`}
              onClick={() => set("level", l.key)}
            >
              <span className="option-name mono">{l.key}</span>
              <span className="option-blurb">{l.blurb}</span>
            </button>
          ))}
        </div>
      </section>

      <section className="setting-card">
        <div className="setting-head">
          <h2>Evidence per turn</h2>
          <p className="muted">
            How many evidence items each turn retrieves. More gives the model broader context;
            fewer keeps prompts tight (and further from the context-window ceiling).
          </p>
        </div>
        <div className="setting-slider">
          <input
            type="range"
            min={2}
            max={12}
            step={1}
            value={settings.topK}
            aria-label="Evidence items per turn"
            onChange={(e) => set("topK", Number(e.target.value))}
          />
          <span className="num slider-value">{settings.topK}</span>
        </div>
      </section>

      <section className="setting-card">
        <div className="setting-head">
          <h2>Synthesis &amp; history</h2>
        </div>
        <label className="setting-toggle">
          <input
            type="checkbox"
            checked={settings.synthesize}
            onChange={(e) => set("synthesize", e.target.checked)}
          />
          <span>
            <b>Synthesize answers with the local LLM</b>
            <span className="option-blurb">
              {health?.ollama.reachable
                ? `Ollama reachable (${health.models.llm}).`
                : "Ollama is offline — turns fall back to the evidence template."}{" "}
              Off = deterministic evidence template (no prose, no context carry).
            </span>
          </span>
        </label>
        <label className="setting-toggle">
          <input
            type="checkbox"
            checked={settings.save}
            onChange={(e) => set("save", e.target.checked)}
          />
          <span>
            <b>Save conversations to local history</b>
            <span className="option-blurb">
              Local-only, never leaves this machine; wipe anytime with{" "}
              <code>mem history --clear</code>. Turning this off also disables carried context
              between turns.
            </span>
          </span>
        </label>
      </section>

      <button
        type="button"
        className="btn"
        onClick={() => setSettings({ ...DEFAULT_SETTINGS })}
      >
        Reset to defaults
      </button>
    </>
  );
}
