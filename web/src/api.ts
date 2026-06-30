// Thin typed client over the local read-only API (`mem serve`). Same-origin; no external calls.

export interface EvidenceItem {
  evidence_id: string;
  cited?: boolean;
  source_id: number;
  chunk_id: number;
  source_type: string;
  source_uri: string;
  title: string | null;
  modality: string | null;
  locator: string;
  page: number | null;
  ocr_confidence: number | null;
  scores: { combined: number; vector: number; lexical: number; recency: number; usage: number };
  excerpt?: string;
}

export interface AnswerPayload {
  query: string;
  model: string | null;
  abstained: boolean;
  answer: string | null;
  cited_evidence_ids?: string[];
  invalid_citations?: string[];
  evidence: EvidenceItem[];
  matched_nodes?: { node_id: number; level: number; node_type: string; title: string | null; score: number }[];
}

export interface ConceptView { node_id: number; title: string | null; centrality: number; members: number; }
export interface EpisodeView { node_id: number; title: string | null; time_start: string | null; time_end: string | null; members: number; }
export interface DriftWindow { window_start: string; window_end: string; drift_metric: number; support: number; }
export interface DriftSummary {
  concept_id: number; title: string | null; overall_drift: number; window_count: number;
  support: number; confidence: number; relearning: boolean;
  span_start: string | null; span_end: string | null; evidence_source_uri: string | null;
  windows: DriftWindow[];
}
export interface ForgettingItem {
  node_id: number; level: number; node_type: string; title: string | null;
  risk: number; importance: number; staleness: number; confidence: number;
  support: number; last_touch: string | null; evidence_source_uris: string[];
}
export interface RecallCard {
  node_id: number; level: number; node_type: string; title: string | null;
  question: string; answer: string | null; risk: number; confidence: number;
  generated_by: string; evidence_source_uris: string[];
}
export interface Health {
  db: { path: string; exists: boolean; size_bytes: number };
  extras: { embeddings: boolean; pdf: boolean; ocr: boolean };
  ollama: { base_url: string; reachable: boolean };
  models: { embed: string | null; llm: string; extract: string };
  config: { path: string | null; loaded: boolean };
  connectors: Record<string, number>;
  memory: MemoryStats | null;
}
export interface MemoryStats {
  total_nodes: number;
  nodes_by_level: Record<string, number>;
  nodes_by_type: Record<string, number>;
  edges: number; relates_edges: number; node_embeddings: number;
  distilled_nodes: number; drift_snapshots: number;
  top_central: { node_id: number; level: number; title: string | null; centrality: number }[];
  integrity: { unsupported_count: number; unsupported_ids: number[]; dangling_count: number; dangling_ids: number[] };
}

async function get<T>(path: string, params?: Record<string, string | number | boolean | undefined>): Promise<T> {
  const url = new URL(path, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== "") url.searchParams.set(k, String(v));
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const body = await res.json();
      if (body?.detail) detail = String(body.detail);
    } catch { /* non-JSON error */ }
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

export const api = {
  health: () => get<Health>("/health"),
  ask: (q: string, opts: { profile?: string; level?: string; top_k?: number; use_llm?: boolean }) =>
    get<AnswerPayload>("/ask", { q, ...opts }),
  concepts: (top = 40) => get<{ concepts: ConceptView[] }>("/concepts", { top }),
  timeline: (limit = 80) => get<{ timeline: EpisodeView[] }>("/timeline", { limit }),
  memoryStats: () => get<MemoryStats>("/memory-stats"),
  drift: (top = 40, min_support = 1) => get<{ drift: DriftSummary[] }>("/drift", { top, min_support }),
  forgetting: (level = "all", top = 20) => get<{ level: string; forgetting: ForgettingItem[] }>("/forgetting", { level, top }),
  recall: (level = "all", top = 12) => get<{ level: string; recall: RecallCard[] }>("/recall", { level, top }),
};
