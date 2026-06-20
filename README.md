# ChronosSeat

**A modern data platform for workforce analytics — built on ClickHouse, orchestrated by Dagster, and served through Rill.**

## What Problem Does This Solve?

ChronosSeat ingests workforce data from enterprise systems (SAP ERP, SharePoint, Excel) and transforms it into a queryable, dimensional data model. It is designed for analytics teams who need a self-contained, deployable analytics stack — no cloud dependencies, no vendor lock-in.

Production data arrives as files. The system watches for new files, validates them, ingests them into ClickHouse, and runs a dbt transformation pipeline (bronze → silver → gold). Dashboards are served through Rill. All orchestration is handled by Dagster.

## Architecture

```
[SAP CSV] ──┐
[SharePoint] ──┼──→ data/inbox/ ──→ Dagster sensor ──→ chronos_bronze.*
[Excel] ──────┘                         │
                                        ▼
                              dbt models (SQL)
                                        │
                              ┌─────────┼─────────┐
                              ▼                   ▼
                      chronos_silver.*     chronos_gold.*
                              │                   │
                              ▼                   ▼
                        Rill dashboards    Change request system
                                           + Web portal
```

## Technology Stack

| Layer | Technology | Role |
|-------|-----------|------|
| **OLAP Database** | ClickHouse 25.7+ | Stores all layers (bronze, silver, gold) as separate databases. Client-server architecture enables concurrent multi-user access. |
| **Orchestration** | Dagster 1.13+ | Watches `data/inbox/` for new files via sensors. Materializes dbt models as Dagster assets. Supports scheduling and automation. |
| **Transformation** | dbt-core + dbt-clickhouse | SQL-based Medallion pipeline. Seeds for reference data. Models for bronze mock generators, silver cleaning, and gold dimensional models. |
| **BI / Dashboards** | Rill | Reads from `chronos_gold` ClickHouse database. Serves interactive dashboards over HTTP. |
| **Web Portal** | Next.js (port 2319) | Entity browser + change request interface. Reads/writes ClickHouse via HTTP API. |
| **Language** | Python 3.13 | Dagster assets, sensors, and Python-based transforms. |

## Medallion Architecture

ClickHouse does not support schemas within databases — only databases and tables. Each Medallion layer maps to a separate ClickHouse database:

| Layer | ClickHouse Database | Content |
|-------|-------------------|---------|
| Bronze | `chronos_bronze` | Raw ingested data + mock generators. Seeds: departments, positions. Models: ERP roster, HR allocations, contractor tracking. |
| Silver | `chronos_silver` | Cleaned, joined, deduplicated data. Incremental models where appropriate. |
| Gold | `chronos_gold` | Business-ready dimensional models. SCD Type 2 dimensions. Fact tables. This is what Rill reads. |

## Project Structure

```
chronos-seat/
├── Developer-Quickstart-ChronosSeat.md   ← Start here for setup instructions
├── chronos-seat-blueprint.md              ← Architecture docs
├── chronos-seat-roadmap.md                ← Phased development plan
│
├── dbt_project/                          ← dbt models, seeds, macros
│   ├── models/
│   │   ├── bronze/                       ← Mock data generators (SQL)
│   │   ├── silver/                       ← Cleaning + conformed transforms
│   │   └── gold/                         ← Dimensional models (SCD Type 2)
│   ├── seeds/
│   │   ├── bronze/                       ← departments.csv, positions.csv
│   │   └── gold/                         ← dim_change_type, dim_change_reason
│   └── macros/                           ← Custom Jinja macros
│
├── src/chronos_seat/                     ← Python + Dagster
│   ├── definitions.py                    ← Root Definitions (assets + resources)
│   └── defs/
│       ├── ingestion/rawgen/             ← File sensors + ingestion assets
│       └── transformation/dbt/           ← DbtProject + DbtCliResource
│
├── rill_dashboard/                       ← Rill project
│   ├── rill.yaml                         ← ClickHouse connector config
│   └── connectors/clickhouse.yaml        ← Connection details
│
├── data/
│   ├── inbox/                            ← Drop zone for production files
│   ├── archive/                          ← Processed files (timestamped)
│   └── change_requests/                  ← Approval workflow directories
│
├── tests/                                ← pytest test suite
├── dagster_home/                         ← Dagster state (gitignored)
├── Dockerfile                            ← Dagster host process image
├── docker-compose.yml                    ← ClickHouse + Dagster + Rill + Portal
├── pyproject.toml                        ├── Python dependencies (uv)
└── Makefile                              ← Common commands
```

## Getting Started

### Prerequisites

- **WSL2 Ubuntu** (Linux native filesystem — `/mnt/c/` is 10x slower)
- **Python 3.13** (`uv python install 3.13 && uv python pin 3.13`)
- **uv** — [install docs](https://docs.astral.sh/uv/getting-started/installation/)
- **Docker + Docker Compose** — for ClickHouse, Rill, and Portal containers
- **Rill** — `curl -s https://rill.sh | sh`
- **Node.js 18+** — for the web portal

### Quick Setup

```bash
# 1. Clone and enter the project
cd ~/workspace/projects/chronos-seat

# 2. Install Python dependencies
uv sync

# 3. Start ClickHouse (Docker)
docker run -d \
  --name clickhouse \
  --ulimit nofile=262144:262144 \
  -p 9000:9000 \
  -p 8123:8123 \
  -v clickhouse-data:/var/lib/clickhouse \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:latest

# 4. Verify ClickHouse is running
sleep 15
clickhouse-client --host localhost --port 9000 --user default --query "SELECT 1"

# 5. Load seed data (departments, positions)
cd dbt_project && uv run dbt seed && cd ..

# 6. Run bronze mock data models
uv run dbt run --select bronze

# 7. Start Dagster
uv run dg dev -h 0.0.0.0 -p 2320
# → http://localhost:2320

# 8. Start Rill (separate terminal)
rill start ./rill_dashboard --port 2321
# → http://localhost:2321
```

### Running Tests

```bash
make test          # pytest + dbt test
make lint          # ruff check
make format        # ruff format
```

## Development Workflow

1. **Scaffold** — Run the scaffold script in §2 of the quickstart
2. **Configure** — Populate `.env`, `pyproject.toml`, `profiles.yml`, `dbt_project.yml`
3. **Start ClickHouse** — Docker container with `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`
4. **Load seeds** — `dbt seed` loads departments and positions into `chronos_bronze`
5. **Run models** — `dbt run` generates mock transactional data
6. **Orchestrate** — Dagster sensors watch `data/inbox/` for production files
7. **Transform** — Silver and gold models clean and dimensionalize the data
8. **Visualize** — Rill reads from `chronos_gold` for dashboards

## Production Deployment

The entire stack runs in Docker containers defined in `docker-compose.yml`:

- **ClickHouse** — `clickhouse/clickhouse-server` (ports 9000, 8123)
- **Dagster webserver + daemon** — built from project Dockerfile (port 2320)
- **Rill** — `ghcr.io/rilldata/rill` (port 2321)

All containers connect via a shared Docker network (`chronos-net`). ClickHouse data persists in a Docker volume (`clickhouse-data`).

## Learn More

- [Developer Quickstart](Developer-Quickstart-ChronosSeat.md) — Step-by-step setup guide
- [Architecture Blueprint](chronos-seat-blueprint.md) — Detailed architecture docs
- [Roadmap](chronos-seat-roadmap.md) — Phased development plan
