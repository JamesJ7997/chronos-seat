# ChronosSeat — Position-Centric Local Lakehouse

A local-first data lakehouse for mapping, tracking, and visualizing position lifecycles. Built on the Duck ecosystem with Dagster orchestration, dbt transformations, and DuckLake storage — running entirely on WSL2 Ubuntu with no cloud dependencies.

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Package Management | `uv` | Fast Python dependency resolution |
| Orchestration | Dagster 1.13 | Asset-based pipelines, lineage, scheduling |
| Storage | DuckLake | ACID-compliant Parquet catalog with time-travel |
| Transformation | dbt Core 1.11 | SQL modularity, `ref()`, incremental models |
| Data Engine | DuckDB 1.5 | Embedded analytical database |
| BI / Dashboards | Rill Developer | BI-as-code YAML dashboards (Phase 2) |
| Deployment | Docker Compose | Portable, reproducible containers |

## Architecture

```
[data/raw]          → Bronze (CSV, Parquet, Excel from Dagster mock assets)
      ↓
[data/silver]       → Silver (Polars cleaning: standardize names, types, casing)
      ↓
[DuckLake catalog]  → Gold (dbt dimensional models: dim_position, dim_employee, facts)
      ↓
[Rill / Portal]     → Dashboards and entity browser
```

**Medallion schema mapping:**
- `bronze` — 1:1 views over raw data (date spine, staging tables)
- `silver` — cleaned, deduplicated, conformed dimensions
- `gold` — business-ready star schema (dims + facts) consumed by Rill

All three schemas live inside a single DuckLake catalog (`dbt_project/data/gold/chronos.ducklake`) using the `attach` pattern, giving every layer Parquet-backed storage and snapshot history.

## Quick Start

### Prerequisites

- WSL2 Ubuntu
- Python 3.13+
- Node.js 18+ (for web portal, Phase 2)

### Install

```bash
# 1. Clone and enter the project
cd ~/workspace/projects
git clone https://github.com/JamesJ7997/chronos-seat.git
cd chronos-seat

# 2. Install Python dependencies
uv sync

# 3. Set up Dagster home
echo "DAGSTER_HOME=$(pwd)/dagster_home" > .env
export DAGSTER_HOME=$(pwd)/dagster_home
```

### Run Mock Data Pipeline

```bash
# Materialize all mock data assets (generates CSV, Parquet, Excel in data/raw/)
uv run dg launch --select "*"
```

### Start Dagster UI

```bash
uv run dg dev -h 0.0.0.0 -p 2320
# Open http://localhost:2320 in your browser
```

### Run dbt (Section 6+)

```bash
# Install dbt packages
cd dbt_project && uv run dbt deps && uv run dbt parse && cd ..

# Materialize all Dagster assets (mock data → silver transforms → dbt build)
uv run dg launch --select "*"
```

## Project Structure

```
chronos-seat/
├── pyproject.toml              # Python package + deps (uv-managed)
├── workspace.yaml              # Dagster workspace config
├── dagster_home/               # Dagster instance (run history, event logs)
│   └── dagster.yaml
├── data/
│   ├── raw/                    # Bronze: CSV, Parquet, Excel from Dagster
│   ├── silver/                 # Silver: cleaned Parquet from Polars transforms
│   └── gold/                   # Gold: DuckLake catalog + Parquet files
├── src/chronos_seat/
│   ├── definitions.py          # Root Dagster Definitions
│   └── defs/
│       ├── ingestion/rawgen/   # Mock data assets + DuckDB resource
│       └── transformation/
│           ├── adhoc/          # Polars silver transforms
│           └── dbt/            # DbtProject, dbt_assets, DbtCliResource
├── dbt_project/
│   ├── dbt_project.yml         # dbt config (schemas, materializations)
│   ├── profiles.yml            # DuckLake attach config
│   ├── packages.yml            # dbt_utils dependency
│   ├── macros/                 # Jinja macros (generate_sk, generate_schema_name)
│   ├── models/
│   │   ├── bronze/             # Views over raw data
│   │   ├── silver/             # Incremental cleaned models
│   │   └── gold/               # Business-ready dims + facts
│   └── seeds/                  # CSV reference data (conformed dimensions)
├── rill_dashboard/             # Rill BI-as-code (YAML sources + dashboards)
├── tests/                      # pytest tests
├── .github/workflows/ci.yml    # CI: lint → test → build
└── docs/
    └── Developer-Quickstart-ChronosSeat.md  # Full developer guide
```

## Development

### Lint

```bash
uv run ruff check src/ tests/        # Check
uv run ruff check src/ tests/ --fix  # Auto-fix
```

### Test

```bash
uv run pytest tests/ -v
```

### Pre-commit

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

## Ports

| Service | Port | URL |
|---|---|---|
| Dagster UI | 2320 | http://localhost:2320 |
| Rill | 2321 | http://localhost:2321 |
| Web Portal | 2319 | http://localhost:2319 |
| FastAPI | 2322 | http://localhost:2322 |

## Roadmap

- **Section 1-5**: ✅ Project scaffold, mock data, Dagster orchestration (complete)
- **Section 6**: dbt transformation layer (models, seeds, DuckLake init)
- **Section 7**: Rill dashboards
- **Section 8-9**: Change request + entity management systems
- **Section 10**: Next.js web portal
- **Section 11**: Docker deployment
- **Section 12**: Testing
