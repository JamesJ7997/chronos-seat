# ChronosSeat Roadmap

> High-level execution plan. Aligned with the QuickStart — not a replacement for it. Usually the order is: **Blueprint → Roadmap → QuickStart**.
>
> **Note**: Backend migrated from DuckLake to ClickHouse (v4 architecture). DuckLake's file-level lock prevented concurrent multi-user access. ClickHouse is a client-server OLAP database that supports concurrent reads and writes natively.

---

## Phase 1 — Foundations
**Goal:** Mirror the QuickStart environment setup and initial DB state.

**Key Actions:**
- Install required stack (Python 3.13, dbt-core, dbt-clickhouse, ClickHouse, FastAPI, Rill)
- Verify `profiles.yml` points to ClickHouse (host, port, schema)
- Confirm the initial schema (bronze, silver, gold) exists and is version-controlled

**QuickStart Alignment:** §§ 1–3 (Env Setup, Scaffolding, Config)

---

## Phase 2 — Data Ingestion & Seeding
**Goal:** Build a reliable pipeline from source files to ClickHouse.

**Key Actions:**
- Implement file ingestion (raw → bronze) using the surrogate-key macro (`generate_sk`)
- Refactor `generate_sk` to use a configurable `effective_date` variable instead of a hard-coded literal
- Create and validate seed data files for core dimensions (e.g., `dim_date`)
- Ensure seeds populate ClickHouse tables correctly

**QuickStart Alignment:** §§ 4–5 (Ingestion Layer, Dagster Orchestration)

---

## Phase 3 — Modeling Layer
**Goal:** Complete the full dbt model hierarchy (staging → intermediate → mart).

**Key Actions:**
- Finish all core entity models (positions, employees, contracts, etc.)
- Add bridge tables and SCD-Type 2 logic where required
- Enforce incremental load patterns and proper schema placement
- Validate model dependencies and data lineage

**QuickStart Alignment:** §§ 6.5–6.7 (Silver Transforms, Staging Models, Mart Models)

---

## Phase 4 — Orchestration & Automation
**Goal:** Automate model execution and data refreshes.

**Key Actions:**
- Create Dagster assets that wrap dbt materializations (table, incremental, external)
- Define daily schedules, sensors, and resources for the pipelines
- Wire partitioning and sorting configurations into the Dagster DAGs
- Integrate dbt assets into the Dagster DAG graph for end-to-end automation

**QuickStart Alignment:** §5 (Dagster Orchestration), §7 (Schedules & Sensors)

---

## Phase 5 — Analytics & Visualization
**Goal:** Deliver actionable insights via dashboards.

**Key Actions:**
- Develop Rill dashboards that consume the gold-layer (mart) tables
- Implement filtering, drill-down, and export capabilities
- Connect dashboards to the orchestrated pipelines for real-time updates

**QuickStart Alignment:** §8 (Rill Dashboards)

---

## Phase 6 — Polish & Scale
**Goal:** Harden the platform for production use and future growth.

**Key Actions:**
- Establish CI/CD pipelines for linting, testing, and deployment
- Add automated test suites for dbt models and Dagster pipelines
- Update the QuickStart documentation to reflect new steps and best practices
- Plan for scaling considerations (larger data volumes, additional dimensions, performance tuning)

**QuickStart Alignment:** §§ 9–12 (Testing, CI/CD, Docker, Deployment)

---

## Timeline (High-Level)

| Phase | Approx. Duration | Primary Deliverables |
|-------|-----------------|----------------------|
| Foundations | 2 weeks | Environment ready, ClickHouse running, initial DB schema in place |
| Data Ingestion & Seeding | 3 weeks | File ingestion pipeline, surrogate-key macro, seed data validated |
| Modeling Layer | 4 weeks | Complete dbt model suite, bridge tables, SCD handling |
| Orchestration & Automation | 4 weeks | Dagster assets, schedules, sensors, integration with dbt |
| Analytics & Visualization | 3 weeks | Rill dashboards, KPI reporting, interactive features |
| Polish & Scale | 2 weeks | CI/CD, automated testing, documentation updates, scaling plan |

---

## Dependencies
- **Phase 2** cannot start until **Phase 1** is verified (catalog and initial schema)
- **Phase 3** relies on completed models from **Phase 2**
- **Phase 4** depends on fully materialized models from **Phase 3**
- **Phase 5** builds on all prior phases but is independent of any single step

## Success Criteria
- A fresh environment can execute the QuickStart steps end-to-end without errors
- Data flows from raw source files through staging, intermediate, and mart layers, ending in queryable ClickHouse tables
- Dagster pipelines run daily with no failures
- All dashboards reflect up-to-date data and are accessible via the Rill UI
- Multiple users can view dashboards and submit change requests simultaneously

---

*Created: 2026-06-18 | Aligned with: chronos-seat-developer-quickstart.md (v6)*
