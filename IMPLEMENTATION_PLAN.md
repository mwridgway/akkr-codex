# REQUIREMENTS Implementation Plan

## Scope and Objectives
Deliver the metrics and workflows defined in `REQUIREMENTS.md` by building a reproducible data pipeline, metric computation layer, and Streamlit reporting experience for CS:GO/Valorant-style match analytics. The plan assumes the mandated stack: Python 3.11 with Polars, Streamlit UI, and AutoGen/Gemini automation.

## Phase 1 – Data Foundations
1. **Inventory Inputs:** Catalog available demo files, positional streams, utility events, and map zone annotations. Flag gaps (e.g., missing bombsite polygons) and prioritize acquisition.
2. **Ingestion Pipeline:** Prototype a Poetry-managed module `src/cs2_analytics/data_io/demos.py` that wraps `awpy` or `demoparser2` to emit normalized event tables (ticks, kills, grenades, player states).
3. **Storage Layout:** Define Parquet datasets under `data/processed/` partitioned by match, map, and round. Enable predicate pushdown by standardizing column names and data types.
4. **Indexing & Metadata:** Implement offset indices and bloom filters where supported; persist dataset manifests (`data/processed/_manifest.json`) to power discovery and incremental loads.

## Phase 2 – Metric Computation Modules
1. **Module Skeleton:** Create `src/cs2_analytics/data_processing/metrics/` with submodules per requirement category (pacing.py, aggression.py, rotations.py, utility.py, tactics.py).
2. **Pacing Metrics:** Implement pure functions that compute Time to First Kill, Time to Bomb Plant, and Average Death Timestamp using tick granularity. Accept Polars LazyFrames to keep transformations composable.
3. **Aggression Metrics:** Use map zone annotations to calculate T-Side Average Distance to Bombsite, CT Forward Presence Count, and Player Spacing. Introduce helper for spatial distance caching.
4. **Rotational Efficiency:** Model rotation triggers and travel times; add a `RotationAnalyzer` class that tracks rotation success and engagement outcomes with configurable thresholds.
5. **Execute & Retake Metrics:** Detect executes via burst-utility heuristics, compute entry success and trade efficiency, and summarize post-plant/retake success rates.
6. **Utility Metrics:** Build a utility impact engine that aggregates flash blind time, flash assists, incendiary damage, position denial, and composite scores via configurable weights.

## Phase 3 – Visualization & Reporting
1. **Data Access Layer:** Add `src/cs2_analytics/services/metric_service.py` to orchestrate metric pulls and caching.
2. **Streamlit Views:** Design dashboard sections per metric category, surface trend charts, heatmaps, and tables, and expose filters for map, side, and time windows.
3. **Coach-Facing Reports:** Generate downloadable CSV/PDF summaries and embed explanatory tooltips describing metric definitions.

## Phase 4 – Testing & Validation
1. **Synthetic Fixtures:** Populate `data/fixtures/` with curated rounds that exercise edge cases (overtime rotations, failed executes, utility overlaps).
2. **Unit Tests:** For each metric function, create pytest modules mirroring source structure with schema, dtype, and sentinel assertions.
3. **Integration Tests:** Validate pipeline end-to-end via `tests/integration/test_metric_pipeline.py`, ensuring ingestion → computation → aggregation works on sample demos.
4. **Coverage & CI:** Enforce >=85% coverage on touched modules and add coverage gates to CI config.

## Phase 5 – Automation & Operations
1. **Scripts:** Extend `scripts/` with idempotent commands (`refresh_metrics.sh`, `generate_reports.sh`) that chain ingestion, metric computation, and report export.
2. **Gemini Prompts:** Version reusable automation prompts under `prompts/` (e.g., `metrics-refactor.gemini`) with README notes for invocation.
3. **Monitoring:** Log pipeline runtimes and output statistics; flag metric drift or missing data via lightweight alerts (Slack/email hooks).

## Milestones & Sequencing
1. **M1:** Data foundations solidified, ingestion pipeline operational, and processed datasets reproducible.
2. **M2:** Core metric modules deliver pacing, aggression, rotation, utility, and execute/retake outputs with tests.
3. **M3:** Streamlit dashboards and reporting tools live with stakeholder feedback loop.
4. **M4:** Automation scripts, CI coverage gates, and monitoring deployed; project ready for iterative refinements.

## Estimated Timeline & Effort
Assuming a core team of two engineers and one data analyst working in two-week sprints:

- **Sprint 1 (Weeks 1-2):** Execute Phase 1 deliverables; expected effort ~1.5 engineer sprints plus analyst availability for data audits.
- **Sprint 2 (Weeks 3-4):** Complete Phase 2 steps 1-3 (pacing, aggression metrics); effort ~2 engineer sprints due to spatial logic.
- **Sprint 3 (Weeks 5-6):** Finish Phase 2 steps 4-6 and begin Phase 3 data access layer; effort ~2 engineer sprints with analyst support for validation heuristics.
- **Sprint 4 (Weeks 7-8):** Wrap Phase 3 dashboards and coach reports, run usability reviews, and start Phase 4 fixtures; effort ~1.5 engineer sprints plus 0.5 analyst sprint.
- **Sprint 5 (Weeks 9-10):** Finalize Phase 4 testing, establish coverage gates, and complete Phase 5 automation; effort ~1.5 engineer sprints and DevOps support for monitoring hooks.
- **Buffer (Weeks 11-12):** Reserve for integration feedback, performance tuning, or scope adjustments; allocate 1 engineer sprint for contingency tasks.

Total expected calendar time: 10-12 weeks including buffer, with ~8 dedicated engineer sprints and ~2 analyst sprints.
