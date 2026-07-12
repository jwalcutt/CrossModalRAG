// Client-side retrieval/generation settings (the Settings view), persisted in
// localStorage and applied to every chat request. These map 1:1 onto the
// parameters the local API accepts per request — engine-level knobs
// (CMRAG_LLM_NUM_CTX, CMRAG_MIN_EVIDENCE_SCORE, …) live in .env, not here.

export interface ChatSettings {
  /** Hybrid retrieval blend: semantic / lexical / recency / usage weighting. */
  profile: "balanced" | "relevant" | "recent" | "usage";
  /** Retrieval entry level; memory levels drill down to L0 for grounding. */
  level: "evidence" | "event" | "episode" | "concept";
  /** Evidence items retrieved per turn (more context vs. tighter prompts). */
  topK: number;
  /** LLM synthesis on/off (off = deterministic evidence template, no context carry). */
  synthesize: boolean;
  /** Persist turns to local chat history (off also disables context carry). */
  save: boolean;
}

export const DEFAULT_SETTINGS: ChatSettings = {
  profile: "relevant",
  level: "evidence",
  topK: 5,
  synthesize: true,
  save: true,
};

const KEY = "cmrag-chat-settings";

export function loadSettings(): ChatSettings {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return { ...DEFAULT_SETTINGS };
    const parsed = JSON.parse(raw) as Partial<ChatSettings>;
    return sanitizeSettings(parsed);
  } catch {
    return { ...DEFAULT_SETTINGS };
  }
}

export function saveSettings(settings: ChatSettings): void {
  localStorage.setItem(KEY, JSON.stringify(settings));
}

export function sanitizeSettings(raw: Partial<ChatSettings>): ChatSettings {
  const profiles = ["balanced", "relevant", "recent", "usage"] as const;
  const levels = ["evidence", "event", "episode", "concept"] as const;
  const topK = Number(raw.topK);
  return {
    profile: profiles.includes(raw.profile as never) ? (raw.profile as ChatSettings["profile"]) : DEFAULT_SETTINGS.profile,
    level: levels.includes(raw.level as never) ? (raw.level as ChatSettings["level"]) : DEFAULT_SETTINGS.level,
    topK: Number.isFinite(topK) ? Math.min(12, Math.max(2, Math.round(topK))) : DEFAULT_SETTINGS.topK,
    synthesize: raw.synthesize !== false,
    save: raw.save !== false,
  };
}
