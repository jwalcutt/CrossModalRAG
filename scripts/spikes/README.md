# Spikes (throwaway measurement scripts — NOT product)

Scripts in this directory are **time-boxed measurement experiments**, not part of the CrossModalRAG
product surface. They may import optional/heavy extras, are not covered by the test suite, and must
**never** be imported by `src/crossmodalrag/`. Their job is to produce a number that informs a  
decision. 

- `clip_dual_space_spike.py` — Measures whether native CLIP image embeddings rescue  
the visually-dominant eval slice that OCR-text-first fails, and whether a dual-space rank-time merge  
(Reciprocal Rank Fusion) is feasible without regressing the text slice. Requires the  
`[image-embeddings]` + `[ocr]` extras. **Measurement only — adoption is a separate, gated step.**

