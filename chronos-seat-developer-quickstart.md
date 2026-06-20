---
date: 2026-06-19
tags:
  - chronos-seat
  - quickstart
---
# ChronosSeat — Developer Quickstart v3

> **Purpose**: Go from zero to fully functional local ChronosSeat installation.
> **Audience**: Developer who will write all code themselves.
> **Prerequisites**: WSL2 Ubuntu, Python 3.13, Node.js 18+, Docker + Docker Compose, Rill Developer. Basic familiarity with ClickHouse/dbt/Dagster concepts.
> **Time estimate**: 4-6 hours for a working local demo.
> **Last updated**: 2026-06-19 — v6. Backend migrated from DuckLake to ClickHouse. DuckLake's file-level lock prevented concurrent multi-user access (Rill dashboard + change requests). ClickHouse is a client-server OLAP database supporting concurrent reads/writes. All connection strings, dbt profiles, Dagster resources, and Docker config updated.

---

## Table of Contents

1. [[#1. Environment Setup]]
2. [[#2. Project Scaffolding]]
3. [[#3. Configuration Files]]
4. [[#4. Mock Data Generator]]
5. [[#5. Dagster Orchestration]]
6. [[#6. dbt Transformation Layer]]
7. [[#7. Rill Dashboards]]
8. [[#8. Change Request System]]
9. [[#9. Entity Management System]]
10. [[#10. Web Portal]]
11. [[#11. Docker Deployment]]
12. [[#12. Testing]]
13. [[#13. Network Access]]
14. [[#14. Scaling Path]]
15. [[#15. Quick Reference — Run Everything]]

---

## 1. Environment Setup

→ [[#Table of Contents]]
→ [[#1.1 Install uv]]
→ [[#1.2 Create Project Workspace]]
→ [[#1.3 Install Rill]]
→ [[#1.4 Verify Installations]]

### 1.1 Install uv

→ [[#1. Environment Setup]]

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
```

### 1.2 Create Project Workspace

→ [[#1. Environment Setup]]

```bash
cd ~/workspace/projects
uv init chronos-seat --app
cd chronos-seat
```

### 1.3 Install Rill

→ [[#1. Environment Setup]]

```bash
curl -s https://rill.sh | sh
```

### 1.4 Verify Installations

→ [[#1. Environment Setup]]

```bash
uv --version
rill version
docker --version
```

> ⚠️ **WSL2 Performance**: Keep the project inside `~/workspace/` (Linux native filesystem). Building on `/mnt/c/` causes 10x slower I/O.

> **⚠️ Python Version:** dbt-core currently requires Python ≤3.13. If your system has 3.14, pin 3.13:
> ```bash
> uv python install 3.13
> uv python pin 3.13
> ```
> This creates a `.python-version` file so `uv` always uses 3.13 for this project.

---

## 2. Project Scaffolding

→ [[#Table of Contents]]

Run this script from the project root:

```bash
#!/usr/bin/env bash
cd ~/workspace/projects/chronos-seat
set -e

echo "🚀 Scaffolding ChronosSeat directory structure..."

# uv init the project
uv init . --app --python 3.13

# if needed, use this to do a clean sweep of all files but .git and .gitignore
#find . -maxdepth 1 -not -name '.git' -not -name '.gitignore' -not -name '.' -exec rm -rf {} +

# Data directories (inbox for file-based ingestion, request workflows)
mkdir -p data/inbox
mkdir -p data/archive
mkdir -p data/change_requests/{inbox,approved,rejected,processing,archive}
mkdir -p data/entity_requests/{inbox,approved,rejected,processing,archive}

# dbt project — use dbt init to create the project scaffold
# This creates dbt_project/, models/, macros/, seeds/, analysis/, dbt_project.yml, profiles.yml
uv add dagster-dbt dbt-core dbt-clickhouse clickhouse-connect faker 
uv run dbt init dbt_project
touch dbt_project/profiles.yml
touch dbt_project/packages.yml
mkdir -p dbt_project/seeds/bronze dbt_project/seeds/gold
touch dbt_project/seeds/bronze/.gitkeep
touch dbt_project/seeds/gold/.gitkeep

# dbt init prompts interactively — accept defaults, name it "dbt_project"
# After init, the project structure is in place with sample files

# Rill — use rill init to create the project scaffold
# This creates rill_dashboard/, rill.yaml, sources/, metrics/
rill init rill_dashboard
# rill init prompts interactively — accept defaults

# Dagster — use create-dagster to scaffold the project
# This creates src/chronos_seat/, definitions.py, defs/, tests/
# It prompts: "Run uv sync?" → choose "n" (uv add runs below, after all inits)
uvx create-dagster@latest project .
mkdir -p dagster_home
touch dagster_home/dagster.yaml 
touch dagster_home/packages.yaml 

# Note: folder name "chronos-seat" becomes package name "chronos_seat" (dashes → underscores)
# This creates:
#   src/chronos_seat/__init__.py
#   src/chronos_seat/definitions.py   ← your root Definitions object goes here
#   src/chronos_seat/defs/__init__.py
#   tests/__init__.py
#   pyproject.toml  (overwritten below by uv add — that is fine)

# Install Python dependencies — MUST run AFTER all init commands above
# (dbt init, rill init, and create-dagster all generate pyproject.toml;
#  this command overwrites it with the correct dependencies)
uv add dagster dagster-webserver dagster-dbt dagster-clickhouse dbt-core dbt-clickhouse clickhouse-connect faker polars pyarrow openpyxl xlsxwriter

# After running, create the subdirectories for ingestion and transformation:
mkdir -p src/chronos_seat/defs/ingestion/rawgen
mkdir -p src/chronos_seat/defs/transformation/dbt
touch src/chronos_seat/defs/ingestion/__init__.py
touch src/chronos_seat/defs/ingestion/rawgen/{__init__.py,assets.py,resources.py}
touch src/chronos_seat/defs/transformation/{__init__.py}
touch src/chronos_seat/defs/transformation/dbt/{__init__.py,assets.py,resources.py}

# Tests (create-dagster already created tests/, add the test files)
touch tests/{test_ingestion.py,test_scd2_constraints.py,test_dbt_transforms.py}

# Config + deployment
mkdir -p .github/workflows nginx/certs
touch .github/workflows/ci.yml
touch {Dockerfile,docker-compose.yml,.dockerignore,.gitignore,.pre-commit-config.yaml}
touch {workspace.yaml,Makefile,pyproject.toml,README.md}
# Note: dagster.yaml lives inside dagster_home/ (created by `make setup` below)
# Note: uv.lock is auto-generated by `uv sync` — do not create it manually

# Ensure data dirs are tracked by git
touch data/.gitkeep
touch data/change_requests/{inbox,approved,rejected,processing,archive}/.gitkeep
touch data/entity_requests/{inbox,approved,rejected,processing,archive}/.gitkeep

# environment file
touch .env

echo "✅ Directory structure created."
echo ""
echo "Next steps:"
echo "  1. Follow §3 Configuration Files to populate .env, pyproject.toml, etc."
echo "  2. Follow §2.1 to start ClickHouse"
```

---

### 2.1 Start ClickHouse

→ [[#2. Project Scaffolding]]

ClickHouse must be running before `dbt seed` or `dbt run`. Start it with Docker:

```bash
# Pull and run ClickHouse server (no password for local dev)
# CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 grants the default user network access
docker run -d \
  --name clickhouse \
  --ulimit nofile=262144:262144 \
  -p 9000:9000 \
  -p 8123:8123 \
  -v clickhouse-data:/var/lib/clickhouse \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:latest

# Wait for it to be ready (about 10-15 seconds)
sleep 15

# Verify it is running
docker exec clickhouse clickhouse-client --query "SELECT version()"

# Verify the default user can connect remotely (no password)
sudo apt install clickhouse-client
clickhouse-client --host localhost --port 9000 --user default --query "SELECT 1"

# install clickhouse cli (doesn't seem to work with the docker server but maybe for local use)
# https://clickhouse.com/docs/install/clickhousectl
curl https://clickhouse.com/cli | s
```

> **Note:** `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` enables the `default` user to connect over the network without a password. This is safe for local development. For production, set a password via `CLICKHOUSE_PASSWORD` and update §3.5 profiles.yml accordingly. Port 9000 is the native protocol (used by dbt-clickhouse), port 8123 is HTTP (used by Rill and the Portal). To stop: `docker stop clickhouse`. To restart: `docker start clickhouse`.

---

## 3. Configuration Files

→ [[#Table of Contents]]

### 3.1 pyproject.toml

→ [[#3. Configuration Files]]

```toml
# ============================================================
# Project metadata — defines the Python package identity.
# uv and pip read this to install the project and its deps.
#
# NOTE: All dependencies listed below are already installed by the
# `uv add` command in step 1.2. This file is the source of truth —
# `uv sync` reads here and generates uv.lock automatically.
# ============================================================
[project]
name = "chronos_seat"  # Package name: used by `uv pip install -e .` and in imports
requires-python = ">=3.13"  # Minimum Python: Dagster 1.13+ and Polars 1.41+ need 3.13
version = "0.1.0"  # Semver: start at 0.1.0 for initial development
description = "Position-centric local lakehouse for enterprise workforce tracking"  # Human-readable summary for PyPI/pip

# ============================================================
# Runtime dependencies — installed in all environments (dev + prod).
# Pinned to minimum versions tested with this codebase.
# ============================================================
dependencies = [
    # --- Orchestration ---
    "dagster==1.13.9",          # Core Dagster framework: assets, jobs, sensors, resources
    "dagster-dbt>=0.29.9",      # Dagster-dbt integration: @dbt_assets, DbtCliResource
    "dagster-dg-cli>=1.13.9",   # Dagster CLI (`dg` command): project scaffolding, dev server
    "dagster-clickhouse>=0.29.9",  # Dagster-ClickHouse integration: ClickhouseResource (managed connections)
    "dagster-webserver>=1.13.9",# Dagster UI: asset graph, run history, sensor status

    # --- Transformation ---
    "dbt-core>=1.11.11,<1.12",   # dbt engine: SQL compilation, ref(), incremental models (dagster-dbt 0.29.9 requires <1.12)
    "dbt-clickhouse>=1.8.0",     # dbt adapter for ClickHouse: runs dbt models against ClickHouse

    # --- Data engine ---
    "clickhouse-connect>=0.7.0", # ClickHouse Python client: HTTP-based connection (used by Dagster resource and change requests)

    # --- File I/O ---
    "openpyxl>=3.1.5",          # Read/write Excel (.xlsx) files: contractor tracking input
    "polars>=1.41.2",           # DataFrame library: mock data generation, silver transforms
    "pyarrow>=24.0.0",          # Parquet support: Polars read/write Parquet files
    "xlsxwriter>=3.2.9",        # Write Excel (.xlsx) files: mock contractor tracking output

    # --- Test / Dev ---
    "faker>=30.0.0",            # Fake data generation: mock employee names, contractor names
]

# ============================================================
# Dev-only dependencies — installed with `uv sync --group dev`.
# Not included in Docker production image.
# ============================================================
[dependency-groups]
dev = [
    "dagster-dg-cli",           # Already in runtime deps; listed here for `uv sync --group dev` completeness
    "dagster-webserver",        # Already in runtime deps; needed for local dev even in dev-only installs
    "pytest>=8.0.0",            # Test runner: `make test`, CI pipeline
    "ruff>=0.5.0",              # Python linter + formatter: `make lint`, `make format`
    "sqlfluff>=3.0.0",          # SQL linter: checks dbt models for style/errors
    "pre-commit>=3.7.0",        # Git hook manager: runs ruff/sqlfluff before every commit
]

# ============================================================
# Build system — tells uv/pip how to build the package.
# Hatchling is uv's default; explicit here for CI reproducibility.
# ============================================================
[build-system]
requires = ["hatchling"]       # Build backend: uv installs this to create the wheel
build-backend = "hatchling.build"  # Entry point: hatchling's build function

# ============================================================
# Hatch build config — ensures pyproject.toml is included in the wheel.
# Needed so `dg` CLI can read project metadata at runtime.
# ============================================================
[tool.hatch.build.targets.wheel]
force-include = { "pyproject.toml" = "pyproject.toml" }  # Ship pyproject.toml inside the wheel package

# ============================================================
# Ruff — Python linter + formatter config.
# ============================================================
[tool.ruff]
target-version = "py313"       # Generate Python 3.13-compatible code (walrus operator, etc.)
line-length = 100              # Max line length: matches Dagster community style

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors: indentation, whitespace, syntax issues
    "F",   # PyFlakes: unused imports, undefined names, logic errors
    "I",   # isort: import ordering and grouping
    "N",   # pep8-naming: class/function/variable naming conventions
    "W",   # pycodestyle warnings: trailing whitespace, blank line issues
    "UP",  # pyupgrade: modernize syntax (e.g., str.format → f-strings)
]

# ============================================================
# Pytest — test runner config.
# ============================================================
[tool.pytest.init_options]
testpaths = ["tests"]          # Directory where pytest discovers test files

# ============================================================
# Dagster DG CLI — project type and module registration.
# `dg` uses this to know where your Dagster code lives.
# ============================================================
[tool.dg]
directory_type = "project"     # Tells `dg` this is a Dagster project (not a workspace)

[tool.dg.project]
root_module = "chronos_seat"   # Python module where definitions.py lives
registry_modules = [
    "chronos_seat.components.*",  # Auto-discover Dagster components in this namespace
]
```

### 3.2 dagster.yaml

→ [[#3. Configuration Files]]

> **Location**: `dagster_home/dagster.yaml` — inside the `dagster_home/` directory, not at the project root.
> Dagster reads this file from `$DAGSTER_HOME/dagster.yaml` at startup.

```yaml
# ============================================================
# Dagster instance config — controls how Dagster stores run data,
# launches jobs, and schedules sensors.
# ============================================================

# --- Run launcher ---
run_launcher:
  module: dagster
  class: DefaultRunLauncher  # Launches runs in-process (same container); use QueuedRunLauncher for production

run_coordinator:
  module: dagster
  class: QueuedRunCoordinator  # Queues runs for sequential execution; add max_concurrent_runs to enable parallelism

# --- Schedules ---
schedules:
  use_threads: true   # Run schedule evaluations in threads (not processes) — lighter weight
  num_workers: 4       # Max concurrent schedule evaluations; 4 is enough for this project

# --- Sensors ---
sensors:
  use_threads: true   # Run sensor evaluations in threads
  num_workers: 4       # Max concurrent sensor evaluations; handles change_request + entity_request sensors

# --- Telemetry ---
telemetry:
  enabled: false       # Disable Dagster telemetry — no usage data sent to Dagster Cloud
```

### 3.3 workspace.yaml

→ [[#3. Configuration Files]]

```yaml
# ============================================================
# Dagster workspace config — tells Dagster where to find code.
# The `dg` CLI reads this to discover assets, jobs, and sensors.
# ============================================================
load_from:
  - python_module:
      module_name: src.chronos_seat.definitions
```

### 3.4 dbt_project/dbt_project.yml

→ [[#3. Configuration Files]]

```yaml
# ============================================================
# dbt project config — defines the dbt project identity,
# folder structure, and materialization defaults.
# Schema names follow Medallion architecture: bronze/silver/gold.
# ============================================================
name: 'chronos_seat'       # dbt project name: used in schema naming and logging
version: '1.0.0'           # dbt project version (not the app version)
config-version: 2          # dbt config schema version: v2 is current as of dbt 1.11+

profile: 'chronos_seat'    # References the profile name in profiles.yml (defines the database connection)

# --- Folder paths ---
model-paths: ["models"]        # Where dbt looks for .sql model files
analysis-paths: ["analysis"]   # Where dbt looks for ad-hoc .sql analysis files
test-paths: ["tests"]          # Where dbt looks for custom data tests
seed-paths: ["seeds"]          # Where dbt looks for CSV seed files (dim_date, dim_change_type, etc.)
macro-paths: ["macros"]        # Where dbt looks for Jinja macros (generate_sk, etc.)

# --- Build output ---
target-path: "target"         # Where dbt puts compiled SQL, manifest.json, run results
clean-targets:                 # Directories cleaned by `dbt clean`
  - "target"                   # Remove compiled artifacts
  - "dbt_packages"             # Remove installed dbt packages (reinstalled by `dbt deps`)

# --- Seed configuration ---
# Seeds are dimension/fact reference data (conformed dimensions) consumed by
# gold-layer models. They live in the gold schema alongside dim/fact tables.
# Medallion rule: Gold depends only on Silver. Since gold models join to
# dim_department, dim_change_type, and dim_change_reason, the seeds must also
# be in gold — not bronze or a separate reference schema.
seeds:
  chronos_seat:
    gold:
      +schema: gold             # dim_change_type, dim_change_reason, dim_department (dim_date is a dbt model, not a seed)

    bronze:
      +schema: bronze           # mock data generator departments(.csv) and positions(.csv)

# --- Model materialization defaults by Medallion layer ---
# bronze  = raw ingested data (file-based or mock)
# silver  = cleaned, joined, deduplicated (incremental for performance)
# gold    = business-ready dimensional models (what Rill reads)
models:
  chronos_seat:
    bronze:
      +materialized: view      # Bronze models are views: lightweight, always reflect latest raw data
      +schema: bronze          # Bronze schema in ClickHouse (raw ingestion layer)
    silver:
      +materialized: incremental  # Silver models are incremental: append new rows, don't rebuild
      +schema: silver          # Silver schema in ClickHouse (cleaned/transformed layer)
    gold:
      +materialized: table     # Gold models are full tables: rebuilt each run for clean dimensional data
      +schema: gold            # Gold schema in ClickHouse (dimensional models, what Rill reads)
```

### 3.5 dbt_project/profiles.yml

→ [[#3. Configuration Files]]

```yaml
# ============================================================
# dbt connection profile — defines which database dbt connects to.
# The `target: dev` is the default profile used by `dbt build`.
# ============================================================
chronos_seat:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: chronos          # Default ClickHouse database (used when no +schema is specified)
      host: localhost          # ClickHouse server host
      port: 9000               # ClickHouse native protocol port
      user: default            # ClickHouse user
      password: ""             # ClickHouse password (empty for local dev)
      threads: 4
```

> **How ClickHouse databases map to dbt schemas:** ClickHouse does not support schemas within databases — only databases and tables. The dbt-clickhouse adapter maps each dbt `+schema` value to a separate ClickHouse database. Without the `generate_schema_name` macro, dbt combines the profile `schema` (`chronos`) with the model `+schema` (`bronze`) to produce `chronos_bronze`. This is the expected behavior — each Medallion layer gets its own ClickHouse database: `chronos_bronze`, `chronos_silver`, `chronos_gold`.

### 3.6 dbt_project/packages.yml

→ [[#3. Configuration Files]]

```yaml
# ============================================================
# dbt external packages — installed via `dbt deps`.
# These provide reusable macros and tests.
# ============================================================
packages:
  - package: dbt-labs/dbt_utils   # dbt_utils: provides cross-database macros (surrogate_key, date_spine, etc.)
    version: ">=1.1.0"            # Minimum version: ensures `surrogate_key()` and `date_spine()` are available
```

### 3.7 .env

→ [[#3. Configuration Files]]

```bash
# ============================================================
# Environment variables — loaded by Dagster and Docker Compose.
# Never commit this file to git (listed in .gitignore).
#
# IMPORTANT: DAGSTER_HOME must be a fully qualified absolute path.
# Relative paths (e.g. ./dagster_home) will cause Dagster to fail.
# Use `pwd` to get the absolute path for your environment.
#
# For local development (outside Docker):
#   cd to the project root, then:
#   echo "DAGSTER_HOME=$(pwd)/dagster_home" > .env
#   export DAGSTER_HOME=$(pwd)/dagster_home
#
# For Docker Compose:
#   DAGSTER_HOME is overridden to /app/dagster_home in docker-compose.yml
#   (the Dockerfile copies dagster.yaml and workspace.yaml into /app/dagster_home/).
# ============================================================
DAGSTER_HOME=/home/wolfj/workspace/projects/chronos-seat/dagster_home  # Dagster's working directory (absolute path required)
# CLICKHOUSE_PASSWORD=  # Uncomment if you set a password for ClickHouse in docker-compose.yml
```

### 3.8 .gitignore

→ [[#3. Configuration Files]]

```
# ============================================================
# Git ignore rules — prevents generated files, secrets, and
# OS artifacts from being committed to the repository.
# ============================================================

# --- Data files (generated by pipeline, not source-controlled) ---
# ClickHouse data is managed by the ClickHouse container (Docker volume),
# not stored in the project directory. No local data files to ignore.

# --- Python artifacts ---
# uv virtual environment (rebuilt by `uv sync`)
.venv/
# Python bytecode cache (rebuilt on import)
__pycache__/
# Compiled Python files
*.pyc
# Python package metadata (rebuilt on install)
*.egg-info/
# Python distribution artifacts
dist/
# Python build artifacts
build/

# --- dbt artifacts ---
# dbt compiled SQL, manifest.json, run results (rebuilt by `dbt build`)
dbt_project/target/
# dbt installed packages (rebuilt by `dbt deps`)
dbt_project/dbt_packages/
# dbt execution logs
dbt_project/logs/

# --- Dagster artifacts ---
# Dagster run history, event logs, sensor state (SQLite, rebuilt on start)
dagster_home/
!dagster_home/dagster.yaml

# --- IDE files ---
# JetBrains IDE config
.idea/
# VS Code config
.vscode/
# Vim swap files
*.swp

# --- OS files ---
# macOS folder metadata
.DS_Store
# Windows thumbnail cache
Thumbs.db

# --- Secrets ---
# Environment variables (contains DAGSTER_HOME)
.env
```

### 3.9 .dockerignore

→ [[#3. Configuration Files]]

```
# ============================================================
# Docker ignore rules — prevents unnecessary files from being
# copied into Docker images (smaller builds, faster deploys).
# ============================================================

# Git history (not needed in image)
.git
# GitHub Actions workflows (CI runs outside Docker)
.github
# Virtual environment (rebuilt inside image via `uv sync`)
.venv/
# Python bytecode cache
__pycache__/
# Compiled Python files
*.pyc
# Environment variables (injected at runtime, not baked in)
.env
# Local env overrides (injected at runtime)
.env.local
# Keep .gitkeep to preserve directory structure in the image
!.gitkeep
# Test files (not needed in production image)
tests/
# Documentation (not needed in production image)
*.md
```

---

## 4. Mock Data Generator

→ [[#Table of Contents]]

### 4.1 Sample Raw Data Format

→ [[#4. Mock Data Generator]]

**ERP Roster** (`bronze.erp_roster` table in ClickHouse):

Generated: 80-100 rows with hire dates from 2023 through today. 15% termination rate.

```csv
employee_id,employee_name,employee_type,position_id,position_title,department_id,department_name,cost_center,hire_date,termination_date,source_system
EMP-001,Alice Johnson,FULL-TIME,POS-1001,Sr. Data Engineer,DEPT-ENG,Engineering,CC-5100,2021-03-15,,ERP
EMP-002,Bob Smith,FULL-TIME,POS-1002,Data Analyst,DEPT-ENG,Engineering,CC-5100,2022-07-01,,ERP
EMP-003,Carol Williams,CONTRACTOR,POS-1003,ML Engineer,DEPT-AIML,AI/ML,CC-5200,2024-01-10,,ERP
EMP-004,David Brown,FULL-TIME,POS-1004,Analytics Engineer,DEPT-DATA,Data,CC-5300,2023-06-20,,ERP
EMP-005,Eve Davis,FULL-TIME,POS-1005,Data Engineer,DEPT-ENG,Engineering,CC-5100,2020-11-08,,ERP
EMP-006,Frank Miller,FULL-TIME,POS-1006,Staff Engineer,DEPT-ENG,Engineering,CC-5100,2019-04-22,,ERP
EMP-007,Grace Lee,CONTRACTOR,POS-1002,Data Analyst,DEPT-ENG,Engineering,CC-5100,2025-06-01,,ERP
EMP-008,Henry Wilson,FULL-TIME,POS-1007,VP Engineering,DEPT-ENG,Engineering,CC-5100,2018-01-15,,ERP
```

**HR Allocations** (`bronze.hr_allocations` table in ClickHouse):

Generated: 80-100 rows with messy casing, dates from 2023 through today. 60% ongoing (no end date).

| emp_id | EmpName | pos_id | PosTitle | dept_code | alloc_factor | start_dt | end_dt |
|--------|---------|--------|----------|-----------|-------------|----------|--------|
| EMP-001 | alice johnson | POS-1001 | sr data engineer | DEPT-ENG | 1.0 | 2021-03-15 | |
| EMP-002 | bob smith | POS-1002 | data analyst | DEPT-ENG | 0.5 | 2022-07-01 | |
| EMP-007 | grace lee | POS-1002 | data analyst | DEPT-ENG | 0.5 | 2025-06-01 | |
| EMP-003 | carol williams | POS-1003 | ml engineer | DEPT-AIML | 1.0 | 2024-01-10 | |

**Contractor Tracking** (`bronze.contractor_tracking` table in ClickHouse):

Generated: 80-100 rows with overlapping dates from 2023 through today.

| contractor_id | contractor_name | position_id | start_date | end_date | rate_type |
|---------------|-----------------|-------------|------------|----------|-----------|
| EMP-003 | Carol Williams | POS-1003 | 2024-01-10 | 2025-12-31 | hourly |
| EMP-007 | Grace Lee | POS-1002 | 2025-06-01 | 2025-08-31 | hourly |

### 4.2 Mock Data — Seeds + dbt Models

→ [[#4. Mock Data Generator]]

> **Architecture**: Static reference data (departments, positions) lives in CSV seeds. Transactional mock data (employees, allocations, contractors) is generated by dbt models using ClickHouse SQL. This keeps the reference data in version control and uses ClickHouse's `numbers()` generator for realistic volume.

#### 4.2.1 Seeds — Static Reference Data

**dbt_project/seeds/bronze/departments.csv** — Department reference data. SCD Type 1 (overwrite on refresh).

```csv
department_id,department_name,cost_center
DEPT-ENG,Engineering,CC-5100
DEPT-AIML,AI/ML,CC-5200
DEPT-DATA,Data,CC-5300
DEPT-FIN,Finance,CC-5400
DEPT-HR,Human Resources,CC-5500
DEPT-OPS,Operations,CC-5600
DEPT-MKT,Marketing,CC-5700
DEPT-SALES,Sales,CC-5800
```

**dbt_project/seeds/bronze/positions.csv** — Position reference data. Maps positions to departments.

```csv
position_id,position_title,department_id
POS-1001,Sr. Data Engineer,DEPT-ENG
POS-1002,Data Analyst,DEPT-ENG
POS-1003,ML Engineer,DEPT-AIML
POS-1004,Analytics Engineer,DEPT-DATA
POS-1005,Data Engineer,DEPT-ENG
POS-1006,Staff Engineer,DEPT-ENG
POS-1007,VP Engineering,DEPT-ENG
POS-1008,Data Scientist,DEPT-AIML
POS-1009,Research Scientist,DEPT-AIML
POS-1010,BI Analyst,DEPT-DATA
POS-1011,Data Architect,DEPT-DATA
POS-1012,Financial Analyst,DEPT-FIN
POS-1013,Sr. Financial Analyst,DEPT-FIN
POS-1014,HR Business Partner,DEPT-HR
POS-1015,Recruiter,DEPT-HR
POS-1016,Operations Manager,DEPT-OPS
POS-1017,Marketing Manager,DEPT-MKT
POS-1018,Content Strategist,DEPT-MKT
POS-1019,Account Executive,DEPT-SALES
POS-1020,Sales Engineer,DEPT-SALES
```

Load seeds:

```bash

cd dbt_project 
uv run dbt deps
uv run dbt seed
```

> **Where do seeds land?** The `dbt-clickhouse` adapter maps each `+schema` to a separate ClickHouse database. Without the `generate_schema_name` macro, dbt combines the profile `schema` (`chronos`) with the model `+schema` to produce compound names. Seeds with `+schema: bronze` create tables in `chronos_bronze`. Seeds with `+schema: gold` create tables in `chronos_gold`. Verify with:
> ```bash
> clickhouse-client --query "SHOW DATABASES"
> clickhouse-client --query "show tables from chronos_bronze"
> ```

#### 4.2.2 dbt Models — Mock Transactional Data

##### 4.2.2.1  Mock Data First Names Macro

**dbt_project/models/bronze/mock_first_names.sql**

```sql
{% macro mock_first_names() %}
[
    'James','John','Sarah','Emma','Michael',
    'David','Lisa','Mary','Robert','Jennifer',
    'William','Linda','Patricia','Barbara',
    'Daniel','Matthew','Andrew','Karen'
]
{% endmacro %}
```
##### 4.2.2.2  Mock Data Last Names Macro

**dbt_project/models/bronze/mock_last_names.sql**

```sql
{% macro mock_last_names() %}
[
    'Smith','Johnson','Brown','Davis','Wilson',
    'Moore','Taylor','Thomas','White','Martin',
    'Anderson','Jackson','Harris','Clark',
    'Lewis','Walker','Hall','Young'
]
{% endmacro %}
```
##### 4.2.2.3 ERP Roster Mock Data
**dbt_project/models/bronze/erp_roster.sql** — 100 employees with deterministic random data via ClickHouse `cityHash64`. Hire dates from 2023 through today. 15% termination rate.
- Generates **80–100 employees** (deterministically based on the current day)
- Generates hire dates between **2023-01-01 and today**
- Has a **15% termination rate**
- Keeps the data reproducible
- Avoids alias reuse issues

```sql
{{
    config(
        materialized='table'
    )
}}
WITH
employee_count AS (
    SELECT
        80 + (toDayOfYear(today()) % 21) AS cnt
),
employees AS (
    SELECT
        number + 1 AS employee_num
    FROM numbers(
        (SELECT cnt FROM employee_count)
    )
),
positions AS (
    SELECT
        *,
        row_number() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
),
employee_base AS (
    SELECT
        employee_num,
        format('EMP-%04d', employee_num) AS employee_id,
        concat(
            arrayElement(
                {{ mock_first_names() }},
                (cityHash64(employee_num) % 18) + 1
            ),
            ' ',
            arrayElement(
                {{ mock_last_names() }},
                (cityHash64(employee_num * 17) % 18) + 1
            )
        ) AS employee_name,
        arrayElement(
            [
                'FULL-TIME',
                'FULL-TIME',
                'FULL-TIME',
                'CONTRACTOR',
                'INTERN'
            ],
            (cityHash64(employee_num * 13) % 5) + 1
        ) AS employee_type,
        cityHash64(employee_num * 7) % 20 AS pos_idx,
        toDate('2023-01-01')
            + toIntervalDay(
                cityHash64(employee_num * 31)
                % (
                    dateDiff(
                        'day',
                        toDate('2023-01-01'),
                        today()
                    ) + 1
                )
            ) AS hire_date,
        cityHash64(employee_num * 19) % 100 < 15
            AS is_terminated
    FROM employees
)
SELECT
    e.employee_id,
    e.employee_name,
    e.employee_type,
    p.position_id,
    p.position_title,
    d.department_id,
    d.department_name,
    d.cost_center,
    e.hire_date,
    if(
        e.is_terminated,
        toString(
            least(
                e.hire_date
                    + toIntervalDay(
                        30 + (
                            cityHash64(employee_num * 23) % 335
                        )
                    ),
                today()
            )
        ),
        ''
    ) AS termination_date,
    if(
        e.is_terminated,
        'TERMINATED',
        'ACTIVE'
    ) AS employment_status,
    'ERP' AS source_system
FROM employee_base e
JOIN positions p
    ON e.pos_idx = p.pos_idx
JOIN {{ ref('departments') }} d
    ON p.department_id = d.department_id
```

##### 4.2.2.4 HR Allocations Mock Data
**dbt_project/models/bronze/hr_allocations.sql** — 100 allocation records with messy casing. Dates from 2023 through today. 60% ongoing.
- Use **80–100 rows** instead of hardcoded 100.
- Generate dates safely between 2023-01-01 and today.
- Make **60% ongoing** (= 40% have an end date, matching your Python).
- Avoid the `CROSS JOIN (...)` that references `allocation_num` (that won't work reliably in ClickHouse).
- Add more realistic employee names with messy casing.
- Calculate `start_dt` in a CTE and reuse it.

```sql
{{
    config(
        materialized='table'
    )
}}
WITH
allocation_count AS (
    SELECT
        80 + (toDayOfYear(today()) % 21) AS cnt
),
allocations AS (
    SELECT
        number + 1 AS allocation_num
    FROM numbers(
        (SELECT cnt FROM allocation_count)
    )
),
positions AS (
    SELECT
        *,
        row_number() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
),
allocation_base AS (
    SELECT
        allocation_num,
        format(
            'EMP-%04d',
            (cityHash64(allocation_num * 5) % 100) + 1
        ) AS emp_id,
        if(
            cityHash64(allocation_num) % 2 = 0,
            lower(
                concat(
                    arrayElement(
		                {{ mock_first_names() }},
                        (cityHash64(allocation_num * 41) % 18) + 1
                    ),
                    ' ',
                    arrayElement(
		                {{ mock_last_names() }},
                        (cityHash64(allocation_num * 43) % 18) + 1
                    )
                )
            ),
            concat(
                arrayElement(
					{{ mock_first_names() }},
                    (cityHash64(allocation_num * 41) % 18) + 1
                ),
                ' ',
                arrayElement(
					{{ mock_last_names() }},
                    (cityHash64(allocation_num * 43) % 18) + 1
                )
            )
        ) AS EmpName,
        cityHash64(allocation_num * 7) % 20 AS pos_idx,
        arrayElement(
            [0.25, 0.5, 0.75, 1.0, 1.0, 1.0],
            (cityHash64(allocation_num * 11) % 6) + 1
        ) AS alloc_factor,
        toDate('2023-01-01')
            + toIntervalDay(
                cityHash64(allocation_num * 29)
                % (
                    dateDiff(
                        'day',
                        toDate('2023-01-01'),
                        today()
                    ) + 1
                )
            ) AS start_dt,
        cityHash64(allocation_num * 17) % 100 < 40
            AS has_end_date
    FROM allocations
)
SELECT
    a.emp_id,
    a.EmpName,
    p.position_id AS pos_id,
    multiIf(
        cityHash64(a.allocation_num * 3) % 10 < 3,
        lower(p.position_title),
        cityHash64(a.allocation_num * 3) % 10 < 6,
        initcap(p.position_title),
        p.position_title
    ) AS PosTitle,
    p.department_id AS dept_code,
    a.alloc_factor,
    a.start_dt,
    if(
        a.has_end_date,
        toString(
            least(
                a.start_dt
                    + toIntervalDay(
                        30
                        + (
                            cityHash64(a.allocation_num * 19)
                            % 335
                        )
                    ),
                today()
            )
        ),
        ''
    ) AS end_dt
FROM allocation_base a
JOIN positions p
    ON a.pos_idx = p.pos_idx
```

##### 4.2.2.5 Contractor Tracking Mock Data
**dbt_project/models/bronze/bronze_contractor_tracking.sql** — 100 contractor records with overlapping dates from 2023 through today.

```sql
{{
    config(
        materialized='table'
    )
}}
WITH contractors AS (
    SELECT number + 1 AS contractor_num
    FROM numbers(100)
),
positions AS (
    SELECT
        *,
        row_number() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
)
SELECT
    format('CTR-%04d', contractor_num) AS contractor_id,
    concat('Contractor ', contractor_num) AS contractor_name,
    p.position_id,
    start_date,
    if(
        cityHash64(contractor_num * 17) % 100 < 40,
        toString(
            least(
                start_date + toIntervalDay(
                    30 + cityHash64(contractor_num * 19) % 335
                ),
                today()
            )
        ),
        ''
    ) AS end_date,
    arrayElement(
        ['hourly','daily','fixed'],
        (cityHash64(contractor_num * 23) % 3) + 1
    ) AS rate_type
FROM contractors c
JOIN positions p
    ON (cityHash64(contractor_num * 7) % 20) = p.pos_idx
CROSS JOIN (
    SELECT
        toDate('2023-01-01')
        + toIntervalDay(
            cityHash64(contractor_num * 31)
            % dateDiff('day', toDate('2023-01-01'), today())
        ) AS start_date
) AS date_calc
```

Run the models:

```bash
cd dbt_project && uv run dbt run --select bronze
```

> **Why seeds + models instead of Python Dagster assets?** Seeds keep static reference data (departments, positions) in version-controlled CSVs. The dbt models use ClickHouse's `numbers()` generator and `cityHash64` hash to produce deterministic, realistic mock data — no Python dependencies, no Faker, no Polars. The data is generated purely in SQL at dbt run time. In production, file-based ingestion assets (§4.3) replace these models.

### 4.3 Resources

→ [[#4. Mock Data Generator]]

**src/chronos_seat/defs/ingestion/rawgen/resources.py** — ClickhouseResource — provides a shared ClickHouse connection to all Dagster assets. Uses `host: localhost, port: 9000` to connect to the ClickHouse server.

```python
"""Shared resources for ingestion."""

from dagster_clickhouse import ClickhouseResource

# ClickhouseResource manages ClickHouse connections via clickhouse-driver.
# All schemas (bronze, silver, gold) live inside the ClickHouse 'chronos' database.
# Host/port match the clickhouse service in docker-compose.yml.
clickhouse_resource = ClickhouseResource(
    host="localhost",
    port=9000,
    user="default",
    password="",
    database="chronos",
)
```

### 4.5 Definitions

→ [[#4. Mock Data Generator]]

**src/chronos_seat/definitions.py** — start with just the dbt assets. Mock data is generated by dbt seeds + models (section 4.2), not Python Dagster assets:

```python
"""Root Dagster definitions — merges all assets, resources, and sensors."""

from dagster import Definitions, load_assets_from_modules
from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import (
    dbt_models,
)  # @dbt_assets from DbtProject
from chronos_seat.defs.transformation.dbt.resources import dbt_resource

all_assets = [
    dbt_models,  # dbt models as Dagster assets (bronze → silver → gold)
]

defs = Definitions(
    assets=all_assets,
    resources={
        "clickhouse": clickhouse_resource,  # ClickhouseResource from dagster-clickhouse (managed connections)
        "dbt": dbt_resource,                # DbtCliResource — runs dbt commands from Dagster
    },
)
```

### 4.6 Running the Mock Data

→ [[#4. Mock Data Generator]]

After creating the seed CSVs and model SQL files in section 4.2, load the mock data:

```bash
# Load seed data (departments, positions)
cd dbt_project && uv run dbt seed

# Run bronze mock data models (erp_roster, hr_allocations, contractor_tracking)
uv run dbt run --select bronze

# Or run everything (seeds + all models)
uv run dbt build
```

> **Note:** The mock data is generated by ClickHouse SQL in dbt models — no Python Dagster assets needed. In production, file-based ingestion assets (§4.3) replace these models.

# Make sure the "chronos" database exists prior to running dbt
clickhouse-client --host localhost --port 9000 --user default --query "CREATE DATABASE IF NOT EXISTS chronos;"

# Start Dagster — dg dev starts BOTH the webserver and daemon.
uv run dg dev -h 0.0.0.0 -p 2320

# In the Dagster UI (http://localhost:2320):
#   1. Go to "Assets" tab
#   2. Select all bronze models (bronze_erp_roster, bronze_hr_allocations, bronze_contractor_tracking)
#   3. Click "Materialize selected"
#   Or materialize all assets at once with the "Materialize all" button
#
# Or materialize via CLI (from project root, Dagster does not need to be running):
uv run dg launch --assets "*"
```

Verify raw files were created:

```bash
clickhouse-client --host localhost --port 9000 --user default --query "SELECT table_name, count() as row_count FROM system.tables WHERE database = 'chronos' GROUP BY table_name"
# Should show: erp_roster, hr_allocations, contractor_tracking
```

At this point you have a working Dagster project with 3 mock assets. The following sections add the remaining assets, sensors, and transformations — each section will update `definitions.py` to include what was just built.

### 4.7 CI/CD — Set Up Continuous Integration

→ [[#4. Mock Data Generator]]

Now that you have working code, set up CI/CD so every push is automatically validated.

**.github/workflows/ci.yml** — GitHub Actions CI pipeline — runs lint (ruff), tests (pytest), and dbt build on every push/PR.

```yaml
# ============================================================
# CI pipeline — runs on every push and PR to main.
# Three-stage pipeline: lint → test → build.
# ============================================================
name: CI

# --- Trigger conditions ---
on:
  push:
    branches: [main]       # Run on direct pushes to main (after merge)
  pull_request:
    branches: [main]       # Run on PRs targeting main (before merge)

# --- Job definitions ---
jobs:
  # Stage 1: Lint — check code quality and formatting
  lint:
    runs-on: ubuntu-latest    # GitHub-hosted runner with Ubuntu
    steps:
      - uses: actions/checkout@v4                    # Check out repo code
      - uses: actions/setup-python@v5                # Install Python 3.13
        with:
          python-version: "3.13"                     # Match project's Python version (dbt-core constraint)
      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh  # uv = fast Python package manager
      - name: Install dependencies
        run: |
          source $HOME/.local/bin/env                # Add uv to PATH
          uv sync                                     # Install all deps from uv.lock
      - name: Lint
        run: |
          source $HOME/.local/bin/env
          uv run ruff check src/ tests/              # Static analysis: catch bugs, unused imports
          uv run ruff format --check src/ tests/     # Format check: ensure consistent style (fails if unformatted)

  # Stage 2: Test — run dbt pipeline + pytest (depends on lint passing)
  test:
    runs-on: ubuntu-latest
    needs: lint                  # Only run if lint job succeeds
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - name: Install uv
        run: curl -LsSf https://astral.sh/uv/install.sh | sh
      - name: Install all dependencies
        run: |
          source $HOME/.local/bin/env
          uv sync                   # Install Python deps
          cd dbt_project && uv run dbt deps   # Install dbt packages (dbt_utils, etc.)
      - name: Run pipeline
        run: |
          source $HOME/.local/bin/env
          cd dbt_project && uv run dbt seed && uv run dbt build   # Load seeds → build all models
      - name: Run tests
        run: |
          source $HOME/.local/bin/env
          cd dbt_project && uv run dbt test   # Run dbt data tests (schema + custom)
          uv run pytest tests/ -v             # Run Python unit/integration tests

  # Stage 3: Build — validate Docker image + compose (depends on test passing)
  build:
    runs-on: ubuntu-latest
    needs: test                  # Only run if test job succeeds
    steps:
      - uses: actions/checkout@v4
      - name: Build Docker image
        run: docker build -t chronos-seat:test .    # Build image to validate Dockerfile syntax
      - name: Validate compose
        run: docker compose config                  # Validate docker-compose.yml (no errors = pass)

```

**.pre-commit-config.yaml** — Pre-commit hooks — ruff linting, trailing whitespace, YAML validation, debug statement checks.

```yaml
# ============================================================
# Pre-commit hooks — run on `git commit` before code is committed.
# Install: `uv run pre-commit install`
# Run manually: `uv run pre-commit run --all-files`
# ============================================================
repos:
  # --- General file cleanup hooks (pre-commit-hooks) ---
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0                              # Pin version for reproducible hooks
    hooks:
      - id: trailing-whitespace              # Remove trailing spaces from all files
      - id: end-of-file-fixer                # Ensure files end with exactly one newline
      - id: check-yaml                       # Validate YAML syntax (fail on malformed YAML)
      - id: check-json                       # Validate JSON syntax
      - id: check-toml                       # Validate TOML syntax (pyproject.toml, etc.)
      - id: check-added-large-files          # Prevent accidentally committing large binaries
        args: ['--maxkb=500']                # Max file size: 500 KB

  # --- Python linting and formatting (ruff) ---
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.5.0
    hooks:
      - id: ruff                             # Static analysis: catch bugs, unused imports, style issues
        args: [--fix]                        # Auto-fix issues where possible (e.g., remove unused imports)
      - id: ruff-format                      # Auto-format code (like black, but faster)

  # --- SQL linting (sqlfluff) ---
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.1.0
    hooks:
      - id: sqlfluff-lint                    # Lint .sql files for style and correctness
        args: [--dialect, clickhouse]          # Use ClickHouse SQL dialect (matches our database)
        files: \.sql$                        # Only run on .sql files (dbt models, analyses)

```

Install pre-commit hooks locally:

```bash
uv run pre-commit install
```

Commit everything to GitHub:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: initial project scaffold with mock assets, CI/CD, and pre-commit hooks"
git push origin main
```

---

## 5. Dagster Orchestration

→ [[#Table of Contents]]

### 5.1 dbt Assets

→ [[#5. Dagster Orchestration]]

**src/chronos_seat/defs/transformation/dbt/project.py** — DbtProject configuration — tells Dagster where the dbt project lives (`dbt_project/`) and how to invoke it.
```python
"""Shared DbtProject instance — used by both assets and resources."""

from pathlib import Path

from dagster_dbt import DbtProject

# DbtProject auto-generates the manifest and points to the dbt project directory
dbt_project = DbtProject(
    project_dir=Path(__file__).resolve().parent.parent.parent.parent.parent.parent / "dbt_project",
    prepare_project_cli_args=["--quiet"],
)
dbt_project.prepare_if_dev()  # Generates manifest.json in dev

```

**src/chronos_seat/defs/transformation/dbt/assets.py** — dbt asset definitions — loads all dbt models from `manifest.json` and creates Dagster assets for each. Enables Dagster to orchestrate dbt runs.
```python
"""Dagster-dbt integration — wraps dbt models as Dagster assets using DbtProject."""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from chronos_seat.defs.transformation.dbt.project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models as Dagster assets."""
    yield from dbt.cli(["build"], context=context).stream()

```


**src/chronos_seat/defs/transformation/dbt/resources.py** — DbtCliResource — the Dagster resource that invokes `dbt` CLI commands. Configured with the project directory and profiles directory.
```python
"""dbt resource — DbtCliResource pointing to the DbtProject directory."""

from chronos_seat.defs.transformation.dbt.project import dbt_project
from dagster_dbt import DbtCliResource

# DbtCliResource runs dbt commands (build, test, etc.) from Dagster
# The project_dir and profiles_dir point to the dbt_project/ directory
dbt_resource = DbtCliResource(
    project_dir=str(dbt_project.project_dir),  # Reuse the DbtProject path
    profiles_dir=str(dbt_project.project_dir),  # profiles.yml lives in dbt_project/
)

```


**Update `src/chronos_seat/definitions.py`** to include the dbt assets and dbt resource:
```python
"""Root Dagster definitions — merges all assets, resources, and sensors."""

from dagster import Definitions, load_assets_from_modules

from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import (
    dbt_models,
)  # @dbt_assets from DbtProject
from chronos_seat.defs.transformation.dbt.resources import dbt_resource

all_assets = [
    dbt_models,  # dbt models as Dagster assets (bronze → silver → gold)
]

defs = Definitions(
    assets=all_assets,
    resources={
        "clickhouse": clickhouse_resource,  # ClickhouseResource from dagster-clickhouse
        "dbt": dbt_resource,  # DbtCliResource - runs dbt commands from Dagster
    },
)
```

---

### 5.2 Running Dagster Assets

→ [[#5. Dagster Orchestration]]

```bash
# Start Dagster UI
uv run dg dev -h 0.0.0.0 -p 2320

# In the UI:
#   1. Assets tab → select all → "Materialize selected"
#   2. This runs: mock data → silver transforms → dbt build
#
# Or via CLI (from project root):
uv run dg launch --assets "*"
```


Commit the Dagster Orchestration:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add dagster orchestration"
git push origin main
```


---

## 6. dbt Transformation Layer

→ [[#Table of Contents]]

> **What `dbt init` already created:** The `dbt init dbt_project` command in §2 created the `dbt_project/` directory with `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`, `seeds/`, `analysis/`, and sample files. You still need to:
> 1. Replace the generated `profiles.yml` with the ClickHouse configuration (§3.4)
> 2. Add the `generate_sk` macro (§6.2)
> 3. Create the staging, intermediate, and mart model SQL files (§6.5–§6.7)
> 4. Add seed files (§6.5)
> 5. Configure `dbt_project.yml` for ClickHouse (§3.4)

### 6.1 Install dbt Packages

→ [[#6. dbt Transformation Layer]]

```bash
cd ~/workspace/projects/chronos-seat/dbt_project
uv run dbt deps
```

### 6.2 Macros

→ [[#6. dbt Transformation Layer]]

**dbt_project/macros/generate_sk.sql** — Surrogate key macro — generates deterministic MD5 hash from natural key + effective date. Used by all SCD Type 2 dimension models.

```sql
{#
  Generate surrogate key from natural key + effective date.
  Uses MD5 hash for deterministic, unique SKs.
#}

{% macro generate_sk(natural_key_column, date_column) %}
  md5(
    coalesce(cast({{ natural_key_column }} as varchar), '') || '-' ||
    coalesce(cast({{ date_column }} as varchar), '')
  )
{% endmacro %}
```

**dbt_project/macros/initcap.sql** — Converts a string to proper case (initial capitalization) while preserving the original punctuation, whitespace, and delimiter formatting. The macro identifies alphanumeric word tokens, capitalizes the first character of each word, lowercases the remaining characters, and then reconstructs the original string using the exact delimiters found between words.

```sql
{#
Converts a string to proper case (initial capitalization) while preserving the original punctuation, whitespace, and delimiter formatting. The macro identifies alphanumeric word tokens, capitalizes the first character of each word, lowercases the remaining characters, and then reconstructs the original string using the exact delimiters found between words.
#} 

{% macro initcap(input_string) %}
    array_to_string(
        list_transform(
            range(
                1,
                array_length(regexp_extract_all({{ input_string }}, '[[:alnum:]]+')) + 1
            ),
            i ->
                upper(left(regexp_extract_all({{ input_string }}, '[[:alnum:]]+')[i], 1))
                || lower(substr(regexp_extract_all({{ input_string }}, '[[:alnum:]]+')[i], 2))
                || coalesce(
                    regexp_extract_all({{ input_string }}, '[^[:alnum:]]+')[i],
                    ''
                )
        ),
        ''
    )
{% endmacro %}
```

### 6.3 Initialize ClickHouse

→ [[#6. dbt Transformation Layer]]

Before running any dbt models or seeds, verify ClickHouse is running and create the database:

```bash
# Verify ClickHouse is running
clickhouse-client --host localhost --port 9000 --user default --query "SELECT version()"

# Create the chronos database
clickhouse-client --host localhost --port 9000 --user default --query "CREATE DATABASE IF NOT EXISTS chronos"
```

Verify the Medallion schemas exist (ClickHouse uses schemas like DuckDB uses databases):

```bash
clickhouse-client --host localhost --port 9000 --user default --query "SHOW SCHEMAS"

# → You should see: chronos (and system)
# Bronze, silver, and gold schemas are managed by dbt — they are created
# when dbt runs, matching the `schema:` values in dbt_project.yml.
```

> **How schemas work in ClickHouse:** ClickHouse doesn't have separate "databases" per layer like DuckLake. The dbt-clickhouse adapter maps `schema:` values from `dbt_project.yml` (bronze, silver, gold) as ClickHouse schemas within the `chronos` database. All three layers live in one ClickHouse database with separate schemas — same logical separation, different physical implementation.

---

### 6.4 Date Dimension

→ [[#6. dbt Transformation Layer]]

**dbt_project/models/marts/dim_date.sql** — 15-year date spine (2020-2034) generated via ClickHouse `generate_series`. No CSV seed needed; this is a dbt model that builds the dimension table directly. Placed in **gold** schema because it's a conformed dimension, alongside the other dimension and fact tables.

```sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_spine AS (
    SELECT
        toDate(concat(toString(2020 + floor(number / 365)), '-01-01')) + toIntervalDay(number % 365) AS full_date
    FROM generate_series(0, 365 * 15) AS number
    WHERE full_date <= toDate('2034-12-31')
)

SELECT
    toYYYYMMDD(full_date) AS date_sk,
    full_date,
    toDayOfWeek(full_date) AS day_of_week,
    formatDateTime(full_date, '%W') AS day_name,
    toDayOfMonth(full_date) AS day_of_month,
    toDayOfYear(full_date) AS day_of_year,
    toWeek(full_date) AS week_of_year,
    toMonth(full_date) AS month_number,
    formatDateTime(full_date, '%M') AS month_name,
    toQuarter(full_date) AS quarter,
    toYear(full_date) AS year,
    toDayOfWeek(full_date) IN (6, 7) AS is_weekend,
    full_date = toLastDayOfMonth(full_date) AS is_month_end,
    full_date = toLastDayOfMonth(full_date)
     AND toMonth(full_date) IN (3, 6, 9, 12) AS is_quarter_end,
    toMonth(full_date) = 12 AND toDayOfMonth(full_date) = 31 AS is_year_end,
    toQuarter(full_date) AS fiscal_quarter,
    toYear(full_date) AS fiscal_year
FROM date_spine
```
ORDER BY full_date
```

Run the date dimension model:

```bash
cd dbt_project
uv run dbt run --select dim_date
```

Verify:

```bash
cd ~/workspace/projects/chronos-seat
clickhouse-client --host localhost --port 9000 --user default --query "SELECT count() AS row_count FROM chronos.dim_date"
# Expected: ~5479 rows (15 years × 365.25 days)
```

> **Why `gold` and not `bronze`?** `dim_date` is a conformed dimension — it's not raw ingested data. It belongs in the gold schema alongside `dim_position`, `dim_employee`, and the fact tables. This matches the Medallion principle: gold = business-ready dimensional models.

---

### 6.5 Seeds (Gold Reference Data)

→ [[#6. dbt Transformation Layer]]

The seeds (`dim_change_type`, `dim_change_reason`, `dim_department`) are conformed dimension tables consumed by gold-layer models. Per Medallion architecture, gold tables must only depend on silver — so these reference dimensions live in the `gold` schema alongside `dim_position`, `dim_employee`, and the fact tables.

**dbt_project/seeds/dim_change_type.csv** — Change type reference data — seed CSV loaded into gold schema. Values: NEW_HIRE, EXIT, TRANSFER, RECLASSIFICATION, etc.

```csv
change_type_sk,change_type_name,change_type_category,description,is_active
NEW_HIRE,New Hire,STAFFING,Employee newly assigned to a position,true
EXIT,Exit,STAFFING,Employee leaves a position,true
CONTRACTOR_OVERLAP,Contractor Overlap,STAFFING,Contractor added to occupied position,true
VACANCY,Vacancy,STAFFING,Position becomes vacant,true
TRANSITION,Transition,STAFFING,Employee moves to different position,true
TITLE_CHANGE,Title Change,STRUCTURE,Position title changes,true
SALARY_CHANGE,Salary Change,FINANCIAL,Salary band adjustment,true
LOCATION_CHANGE,Location Change,STRUCTURE,Position location changes,true
```

**dbt_project/seeds/dim_change_reason.csv** — Change reason reference data — seed CSV loaded into gold schema. Values: HIRING, REPLACEMENT, RESTRUCTURE, etc.

```csv
change_reason_sk,change_reason_name,description,is_active
HIRING,Hiring,New employee onboarding,true
REPLACEMENT,Replacement,Backfilling a departing employee,true
RESTRUCTURE,Restructure,Organizational restructuring,true
CONTRACT_END,Contract End,Contractor engagement ends,true
PROMOTION,Promotion,Employee promoted to new role,true
```

**dbt_project/seeds/dim_department.csv** — Department reference data — seed CSV loaded into gold schema. SCD Type 1 (overwrite on refresh).

```csv
department_sk,department_id,department_name,division,cost_center_lead,is_active
DEPT-ENG,DEPT-ENG,Engineering,Technology,Henry Wilson,true
DEPT-AIML,DEPT-AIML,AI/ML,Technology,Frank Miller,true
DEPT-DATA,DEPT-DATA,Data,Technology,Alice Johnson,true
```

Load seeds:

```bash
cd dbt_project && uv run dbt seed
```

### 6.6 Staging Models

→ [[#6. dbt Transformation Layer]]

Staging models read directly from ClickHouse bronze/silver tables. Since the raw data is already in ClickHouse (written by Dagster assets), we use direct table references instead of `{{ source() }}` — no `sources.yml` needed for bronze.

**dbt_project/models/staging/stg_erp_roster.sql** — Staging view that reads raw `bronze.erp_roster` (SAP ERP employee export) and applies light cleaning: trims whitespace, uppercases IDs and types, preserves all source columns. Materialized as a view in the `silver` schema — no data duplication, just a cleaned projection.

```sql
{{ config(marterialized='view', schema='silver') }}

with source as (
    select * from bronze.erp_roster
),
cleaned as (
    select
        employee_id,
        trim({{ initcap('employee_name') }}) as employee_name,
        UPPER(TRIM(employee_type)) as employee_type,
        UPPER(TRIM(position_id)) as position_id,
        TRIM(position_title) as position_title,
        UPPER(TRIM(department_id)) as department_id,
        TRIM(department_name) as department_name,
        TRIM(cost_center) as cost_center,
        hire_date,
        termination_date,
        source_system,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned
```

**dbt_project/models/staging/stg_hr_allocations.sql** — Staging view that reads `bronze.hr_allocations` (HR allocation data from SharePoint) and standardizes casing: uppercases IDs, initcaps names and titles, preserves allocation factors and date ranges. Materialized as a view in the `silver` schema.

```sql
{{ config(materialized='view', schema='silver') }}

with source as (
    select * from bronze.hr_allocations
),
cleaned as (
    select 
        upper(trim(emp_id)) as employee_id,
        trim({{ initcap('EmpName') }}) as employee_name,
        upper(trim(pos_id)) as position_id,
        trim({{ initcap('PosTitle') }}) as position_title,
        upper(trim(dept_code)) as department_id,
        alloc_factor as allocation_factor,
        start_dt as assignment_start,
        end_dt as assignment_end,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned
```

**dbt_project/models/staging/stg_contractor_tracking.sql** — Staging view that reads `bronze.contractor_tracking` (contractor data from SharePoint) and standardizes casing: uppercases IDs, initcaps names, preserves assignment dates and employee type. Materialized as a view in the `silver` schema.

```sql
{{ config(materialized='view', schema='silver') }}

with source as (
    select * from bronze.contractor_tracking
),
cleaned as (
    select 
        upper(trim(contractor_id)) as employee_id,
        initcap(trim(contractor_name)) as employee_name,
        upper(trim(position_id)) as position_id,
        start_date as assignment_start,
        end_date as assignment_end,
        rate_type as employee_type,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned
```

> **Why no `sources.yml` for bronze?** The old approach used `{{ source('bronze', 'erp_roster') }}` to read from flat files in `data/raw/`. Now that Dagster writes raw data directly into ClickHouse bronze tables, the staging models can reference them directly as `bronze.erp_roster`. No source definitions needed — dbt and ClickHouse share the same connection.

### 6.7 Mart Models (Gold Layer)

→ [[#6. dbt Transformation Layer]]

**dbt_project/models/marts/dim_position.sql** — SCD Type 2 position master dimension. Reads distinct positions from `stg_erp_roster`, generates a surrogate key via `generate_sk()` using a configurable effective start date (`var("dim_effective_start", "2025-01-01")`). Each row represents a unique position with SCD Type 2 tracking columns (`effective_start_date`, `effective_end_date`, `is_current`). Materialized as a table in the `gold` schema.

```sql
{{
    config(
        materialized='table',
        schema='gold',
        unique_key='position_sk'
    )
}}

WITH source AS (
    SELECT DISTINCT
        position_id,
        position_title,
        department_id,
        cost_center
    FROM {{ ref('stg_erp_roster') }}
),

final AS (
    SELECT
        {{ generate_sk('position_id', "'" ~ var("dim_effective_start", "2025-01-01") ~ "'") }} AS position_sk,
        position_id,
        position_title,
        department_id,
        cost_center,
        NULL AS budgeted_salary_band,
        FALSE AS is_manager_position,
        NULL AS manager_sk,
        DATE '{{ var('dim_effective_start', '2025-01-01') }}' AS effective_start_date,
        DATE '9999-12-31' AS effective_end_date,
        TRUE AS is_current,
        'ERP' AS source_system,
        current_timestamp AS inserted_at,
        current_timestamp AS _loaded_at
    FROM source
)

SELECT * FROM final
```

**dbt_project/models/marts/dim_employee.sql** — SCD Type 2 employee master dimension. Reads distinct employees from `stg_erp_roster`, generates a surrogate key from `employee_id` + `hire_date`. Uses the actual hire date as the effective start date — unlike dim_position, this has a real date from the source. Materialized as a table in the `gold` schema.

```sql
{{
    config(
        materialized='table',
        schema='gold',
        unique_key='employee_sk'
    )
}}

with source as (
    select distinct 
        employee_id,
        employee_name,
        employee_type,
        hire_date
    from {{ ref('stg_erp_roster') }}
),
final as (
    select
        {{ generate_sk('employee_id', 'hire_date') }} as employee_sk,
        employee_id,
        employee_name,
        employee_type,
        hire_date,
        Null as termination_date,
        True as is_current,
        hire_date::Date as effective_start_date,
        '9999-12-31'::Date as effective_end_date,
        'ERP' as source_system,
        current_timestamp as inserted_at,
        current_timestamp as _loaded_at
    from source
)
select * from final
```

**dbt_project/models/marts/fact_position_occupancy_event.sql** — Append-only event fact table that records every position occupancy change (new hires, exits, transfers). Reads from `stg_erp_roster` (filtered to FULL-TIME employees), joins to `dim_position` and `dim_employee` to resolve surrogate keys. Uses MD5-based `event_id` from employee + position + date. Materialized as an incremental table in the `gold` schema — only new events are appended on each run.

```sql
{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='event_id'
    )
}}

with source as (
    select 
        employee_id,
        position_id,
        hire_date::Date as effective_date
    from {{ ref('stg_erp_roster') }}
    where employee_type = 'FULL-TIME'
),
events as (
    select 
        md5(s.employee_id || '-' || s.position_id || '-' || cast(s.effective_date as varchar)) as event_id,
        dp.position_id,
        de.employee_sk,
        'NEW_HIRE' as change_type_sk,
        'system' as requested_by,
        Null as approved_by,
        current_timestamp as event_timestamp,
        s.effective_date,
        s.effective_date as requested_date,
        Null as change_reason_sk,
        'Initial load' as change_notes,
        Null as old_value,
        json_object('employee_id', s.employee_id, 'position_id', s.position_id) as new_value,
        1 as event_version,
        Null as superseded_by,
        md5(cast(current_timestamp as varchar)) as batch_id
    from source s
        left join {{ ref('dim_position') }} dp on s.position_id = dp.position_id and dp.is_current = True 
        left join {{ ref('dim_employee') }} de on s.employee_id = de.employee_id and de.is_current = True
)
select * from events

```

**dbt_project/models/marts/bridge_position_occupancy.sql** — Many-to-many bridge table linking employees to positions via HR allocations. Reads from `stg_hr_allocations`, joins to `dim_position` and `dim_employee` to resolve surrogate keys. Includes an `is_overlap` flag that detects when multiple employees are allocated to the same position during the same period. Materialized as a table in the `gold` schema — full refresh each run since allocations change.

```sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with allocations as (
    select
        employee_id,
        position_id,
        assignment_start::Date as assignment_start,
        coalesce(nullif(assignment_end, ''), '9999-12-31')::Date as assignment_end,
        allocation_factor
    from {{ ref('stg_hr_allocations') }}
),
final as (
    select 
        dp.position_sk,
        de.employee_sk,
        a.assignment_start,
        a.assignment_end,
        a.allocation_factor,
        case 
            when exists(
                select 1 from allocations a2
                where a2.position_id = a.position_id
                and a2.employee_id != a.employee_id
                and a2.assignment_start <= a.assignment_end
                and a2.assignment_end >= a.assignment_start
            ) then True
            else False
        end as is_overlap
    from allocations a 
    left join {{ ref('dim_position') }} dp on a.position_id = dp.position_id and dp.is_current = true 
    left join {{ ref('dim_employee') }} de on a.employee_id = de.employee_id and de.is_current = true 
)
select * from final
```

Before pushing changes or reloading Dagster, run these checks:

```bash
cd ~/workspace/projects/chronos-seat/dbt_project
uv run dbt parse && uv run dbt compile && uv run dbt list
```

→ Validates the entire dbt project compiles cleanly: `parse` checks YAML/model syntax, `compile` builds all models (catching ref/source errors), and `list` confirms all models/resources are loadable. If any of these fail, fix before proceeding — a broken dbt project will cause Dagster asset materialization failures.


### 6.8 Schema Tests

→ [[#6. dbt Transformation Layer]]

**dbt_project/models/marts/schema.yml** — Schema tests — dbt data quality tests for gold-layer models (unique, not_null, expression_is_true, relationships).

```yaml
version: 2

models:
  - name: dim_position
    description: "SCD Type 2 position master records"
    columns:
      - name: position_sk
        tests:
          - unique
          - not_null
      - name: position_id
        tests:
          - not_null
    tests:
      - dbt_utils.expression_is_true:
          expression: "effective_start_date <= effective_end_date"

  - name: dim_employee
    description: "SCD Type 2 employee master records"
    columns:
      - name: employee_sk
        tests:
          - unique
          - not_null
      - name: employee_id
        tests:
          - not_null
    tests:
      - dbt_utils.expression_is_true:
          expression: "effective_start_date <= effective_end_date"

  - name: fact_position_occupancy_event
    description: "Append-only event log for position changes"
    columns:
      - name: event_id
        tests:
          - unique
          - not_null

  - name: bridge_position_occupancy
    description: "Many-to-many position occupancy with overlap tracking"
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - position_sk
            - employee_sk
            - assignment_start
```


Commit the dbt Transformation Layer:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add dbt transformation layer"
git push origin main
```


---

## 7. Rill Dashboards

→ [[#Table of Contents]]
→ [[#7.1 rill.yaml]]
→ [[#7.2 Connector clickhouse.yaml]]
→ [[#7.3 Source Configuration]]
→ [[#7.4 Main Dashboard]]
→ [[#7.5 Start Rill]]

> **What `rill init` already created:** The `rill init rill_dashboard` command in §2 created the `rill_dashboard/` directory with `rill.yaml`, `sources/`, `metrics/`, and sample files. You still need to:
> 1. Replace the generated `rill.yaml` with the ClickHouse configuration (§7.1)
> 2. Create the ClickHouse connector YAML (§7.2)
> 3. Define the source model over ClickHouse gold tables (§7.3)
> 4. Create the position tracker metrics view (§7.4)

### 7.1 rill.yaml

→ [[#7. Rill Dashboards]]

```yaml
# rill_dashboard/rill.yaml
compiler: rillv1

display_name: chronos_seat

# The project's default OLAP connector.
# Learn more: https://docs.rilldata.com/reference/olap-engines
olap_connector: clickhouse
```

### 7.2 Connector: clickhouse.yaml

→ [[#7. Rill Dashboards]]

```yaml
# rill_dashboard/connectors/clickhouse.yaml
# Connector YAML
# Reference documentation: https://docs.rilldata.com/developers/build/connectors/olap/clickhouse

type: connector

driver: clickhouse
host: clickhouse
port: 9000
database: chronos
```

### 7.3 Source Configuration

→ [[#7. Rill Dashboards]]

**rill_dashboard/sources/gold_sources.yaml** — Rill source model — defines a SQL model over ClickHouse gold schema for dashboard queries.

```yaml
type: source
connector: clickhouse
sql: |
  SELECT
    dp.position_id,
    dp.position_title,
    dp.cost_center,
    dp.department_id,
    de.employee_id,
    de.employee_name,
    de.employee_type,
    bp.allocation_factor,
    bp.is_overlap,
    fpe.change_type_sk,
    fpe.effective_date
  FROM gold.dim_position dp
  LEFT JOIN chronos.gold.bridge_position_occupancy bp ON dp.position_sk = bp.position_sk
  LEFT JOIN chronos.gold.dim_employee de ON bp.employee_sk = de.employee_sk
  LEFT JOIN chronos.gold.fact_position_occupancy_event fpe ON dp.position_id = fpe.position_id
  WHERE dp.is_current = TRUE
```

### 7.4 Main Dashboard

→ [[#7. Rill Dashboards]]

**rill_dashboard/metrics/position_tracker.yaml** — Rill metrics view — main position tracker dashboard with KPIs and dimensions reading from the gold_sources model.

```yaml
type: metrics_view
display_name: "Position & Headcount Tracking"
model: joined_position_details
timeseries: effective_date

measures:
  - name: total_headcount
    label: "Total Active Headcount"
    expression: "COUNT(DISTINCT employee_id)"
  - name: double_filled
    label: "Double-Filled Seats"
    expression: "COUNT(DISTINCT position_id) FILTER (WHERE is_overlap = TRUE)"
  - name: vacant_seats
    label: "Vacant Seats"
    expression: "COUNT(DISTINCT position_id) FILTER (WHERE employee_id IS NULL)"
  - name: total_positions
    label: "Total Positions"
    expression: "COUNT(DISTINCT position_id)"

dimensions:
  - name: position_title
    label: "Position Title"
    column: position_title
  - name: cost_center
    label: "Cost Center"
    column: cost_center
  - name: employee_type
    label: "Worker Classification"
    column: employee_type
  - name: department_id
    label: "Department"
    column: department_id
  - name: change_type_sk
    label: "Change Type"
    column: change_type_sk
```

### 7.5 Start Rill

→ [[#7. Rill Dashboards]]

```bash
rill start ./rill_dashboard --port 2321
# → http://localhost:2321
```


Commit the Rill Dashboards:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add rill dashboards"
git push origin main
```


---

## 8. Change Request System

→ [[#Table of Contents]]

### 8.1 File Format

→ [[#8. Change Request System]]

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `request_id` | Yes | Unique identifier | `CR-2026-0042` |
| `request_date` | Yes | Date submitted | `2026-06-13` |
| `requested_by` | Yes | Submitter | `jane.smith` |
| `approved_by` | No | Approver (NULL = pending) | `john.doe` |
| `effective_date` | Yes | When change takes effect | `2026-07-01` |
| `change_type` | Yes | Must match dim_change_type | `NEW_HIRE` |
| `change_reason` | No | Must match dim_change_reason | `HIRING` |
| `position_id` | Yes | Natural key | `POS-12345` |
| `employee_id` | No* | Natural key | `EMP-9876` |
| `employee_name` | No | Full name (for new hires) | `Alice Johnson` |
| `employee_type` | No | FULL-TIME, CONTRACTOR, INTERN | `FULL-TIME` |
| `position_title` | No | New title | `Senior Data Engineer` |
| `department_id` | No | Department | `DEPT-ENG` |
| `cost_center` | No | Financial code | `CC-4200` |
| `allocation_factor` | No | For overlaps (1.0 = full) | `1.0` |
| `notes` | No | Free-text justification | `Backfill for Bob's departure` |

\* Optional for VACANCY.

### 8.2 Sample Change Request

→ [[#8. Change Request System]]

**data/change_requests/inbox/CR-2026-0001.csv** — Sample change request file — example CSV showing the format for submitting position/employee changes through the inbox workflow.

```csv
request_id,request_date,requested_by,approved_by,effective_date,change_type,change_reason,position_id,employee_id,employee_name,employee_type,position_title,department_id,cost_center,allocation_factor,notes
CR-2026-0001,2026-06-14,hr.admin,ops.manager,2026-07-01,NEW_HIRE,HIRING,POS-1008,EMP-009,Iris Chen,FULL-TIME,Data Engineer,DEPT-ENG,CC-5100,1.0,New graduate hire
CR-2026-0002,2026-06-14,hr.admin,,2026-07-15,EXIT,,POS-1005,EMP-005,,,,,,,,Eve Davis resignation
CR-2026-0003,2026-06-14,ops.manager,director,2026-08-01,TITLE_CHANGE,PROMOTION,POS-1001,,,,,Sr. Data Engineer,DEPT-ENG,,,Promoted to Staff Engineer
```

### 8.3 Validation Rules

→ [[#8. Change Request System]]

1. Position exists in dim_position (or is new for NEW_HIRE)
2. Employee exists in dim_employee (unless NEW_HIRE with new employee)
3. request_id is unique
4. effective_date >= CURRENT_DATE
5. change_type matches dim_change_type
6. No conflicting active requests for same position_id + employee_id
7. employee_type is FULL-TIME, CONTRACTOR, or INTERN

### 8.4 Dagster Sensor

→ [[#8. Change Request System]]

**src/chronos_seat/defs/ingestion/rawgen/change_request_sensor.py** — Change request sensor — watches `data/change_requests/inbox/` for new CSV files, validates them, and triggers processing.

```python
"""Dagster sensor — watches change_requests/inbox/ for new files."""

from dagster import sensor, RunRequest, DefaultSensorStatus
from pathlib import Path
import polars as pl

INBOX_PATH = Path("data/change_requests/inbox")
APPROVED_PATH = Path("data/change_requests/approved")
REJECTED_PATH = Path("data/change_requests/rejected")


def validate_change_request(file_path: Path) -> tuple[bool, list[str]]:
    """Validate a change request file. Returns (is_valid, errors)."""
    errors = []
    try:
        df = pl.read_csv(file_path)
    except Exception as e:
        return False, [f"Cannot read file: {e}"]

    required_cols = ["request_id", "request_date", "requested_by",
                     "effective_date", "change_type", "position_id"]
    for col in required_cols:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    if errors:
        return False, errors

    for col in required_cols:
        if df[col].is_null().any() or (df[col] == "").any():
            errors.append(f"Empty values in required column: {col}")

    valid_types = {"NEW_HIRE", "EXIT", "CONTRACTOR_OVERLAP", "VACANCY",
                   "TRANSITION", "TITLE_CHANGE", "SALARY_CHANGE", "LOCATION_CHANGE"}
    invalid = set(df["change_type"].to_list()) - valid_types
    if invalid:
        errors.append(f"Invalid change types: {invalid}")

    return len(errors) == 0, errors


@sensor(
    name="change_request_sensor",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def change_request_sensor(context):
    """Watch for new change request files in inbox."""
    for p in [INBOX_PATH, APPROVED_PATH, REJECTED_PATH]:
        p.mkdir(parents=True, exist_ok=True)

    for file_path in INBOX_PATH.glob("*.csv"):
        is_valid, errors = validate_change_request(file_path)
        dest = APPROVED_PATH / file_path.name if is_valid else REJECTED_PATH / file_path.name
        file_path.rename(dest)
        if is_valid:
            yield RunRequest(run_key=f"cr_{file_path.stem}")
        else:
            context.log.warning(f"Rejected {file_path.name}: {errors}")
```

### 8.5 dbt Models

→ [[#8. Change Request System]]

**dbt_project/models/staging/stg_change_requests.sql** — Incremental staging model that reads raw change request files via `{{ source('change_requests', 'raw_change_request') }}`. Cleans and casts all columns: uppercases IDs and types, trims text, safely casts dates and allocation factors with `TRY_CAST`. The incremental filter on `inserted_at` ensures only new change requests are processed on each run.

```sql
{{
    config(materialized='incremental', unique_key='request_id')
}}

WITH source AS (
    SELECT * FROM {{ source('change_requests', 'raw_change_request') }}
    {% if is_incremental() %}
    WHERE inserted_at > (SELECT MAX(inserted_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        request_id,
        TRIM(UPPER(change_type)) AS change_type,
        TRIM(UPPER(change_reason)) AS change_reason,
        TRIM(UPPER(position_id)) AS position_id,
        TRIM(UPPER(employee_id)) AS employee_id,
        TRIM(employee_name) AS employee_name,
        TRIM(UPPER(employee_type)) AS employee_type,
        TRY_CAST(effective_date AS DATE) AS effective_date,
        TRY_CAST(request_date AS DATE) AS request_date,
        TRIM(requested_by) AS requested_by,
        TRIM(approved_by) AS approved_by,
        TRY_CAST(allocation_factor AS DECIMAL(3,2)) AS allocation_factor,
        TRIM(notes) AS notes,
        inserted_at
    FROM source
)

SELECT * FROM cleaned
```

**dbt_project/models/intermediate/int_change_request_events.sql** — Intermediate model that enriches change requests with dimension surrogate keys. Takes the cleaned change requests from `stg_change_requests` and LEFT JOINs to `dim_position` (to resolve `position_sk` and `department_sk`) and `dim_employee` (to resolve `employee_sk`), matching on current rows only (`is_current = TRUE`). This is the last step before the Dagster asset applies the changes to gold-layer fact tables.

```sql
WITH requests AS (
    SELECT * FROM {{ ref('stg_change_requests') }}
),

position_sk AS (
    SELECT cr.request_id, dp.position_sk, dp.department_sk
    FROM requests cr
    LEFT JOIN {{ ref('dim_position') }} dp
        ON cr.position_id = dp.position_id AND dp.is_current = TRUE
),

employee_sk AS (
    SELECT cr.request_id, de.employee_sk
    FROM requests cr
    LEFT JOIN {{ ref('dim_employee') }} de
        ON cr.employee_id = de.employee_id AND de.is_current = TRUE
)

SELECT cr.*, ps.position_sk, ps.department_sk, es.employee_sk
FROM requests cr
LEFT JOIN position_sk ps ON cr.request_id = ps.request_id
LEFT JOIN employee_sk es ON cr.request_id = es.request_id
```


Commit the Change Request System:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add change request system"
git push origin main
```


---

**Update `src/chronos_seat/definitions.py`** to include the change request sensor:

```python
"""Root Dagster definitions — merges all assets, resources, and sensors."""

from dagster import Definitions, load_assets_from_modules
from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import dbt_models  # @dbt_assets from DbtProject
from chronos_seat.defs.transformation.dbt.resources import dbt_resource
from chronos_seat.defs.ingestion.rawgen.sensors import change_request_sensor

all_assets = [
    dbt_models,                                 # dbt models as Dagster assets (bronze → silver → gold)
]

all_sensors = [
    change_request_sensor,  # Watches change_requests/inbox/ for new CSV/Excel files
]

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
    resources={
        "clickhouse": clickhouse_resource,
        "dbt": dbt_resource,
    },
)
```


## 9. Entity Management System

→ [[#Table of Contents]]

### 9.1 File Format

→ [[#9. Entity Management System]]

| Column | Required | Description |
|--------|----------|-------------|
| `request_id` | Yes | Unique identifier |
| `request_date` | Yes | Date submitted |
| `requested_by` | Yes | Submitter |
| `approved_by` | No | Approver (NULL = pending) |
| `effective_date` | Yes | When change takes effect |
| `entity_type` | Yes | POSITION, EMPLOYEE, DEPARTMENT |
| `operation` | Yes | CREATE, UPDATE, DEACTIVATE, REACTIVATE |
| `entity_id` | Yes* | Natural key (optional for CREATE) |
| `field_name` | Yes** | Field to update |
| `old_value` | No | Current value (audit) |
| `new_value` | Yes** | New value |
| `notes` | No | Justification |

### 9.2 Sample Files

→ [[#9. Entity Management System]]

**Multi-row CREATE** (`data/entity_requests/inbox/ENT-2026-0001.csv`):

```csv
request_id,request_date,requested_by,effective_date,entity_type,operation,entity_id,field_name,old_value,new_value,notes
ENT-2026-0001,2026-06-14,hr.admin,2026-07-01,POSITION,CREATE,POS-1009,position_id,,POS-1009,New senior role
ENT-2026-0001,2026-06-14,hr.admin,2026-07-01,POSITION,CREATE,POS-1009,position_title,,Senior Analytics Engineer,New senior role
ENT-2026-0001,2026-06-14,hr.admin,2026-07-01,POSITION,CREATE,POS-1009,department_id,,DEPT-DATA,New senior role
ENT-2026-0001,2026-06-14,hr.admin,2026-07-01,POSITION,CREATE,POS-1009,cost_center,,CC-5300,New senior role
```

**Single-row UPDATE** (`data/entity_requests/inbox/ENT-2026-0002.csv`):

```csv
request_id,request_date,requested_by,approved_by,effective_date,entity_type,operation,entity_id,field_name,old_value,new_value,notes
ENT-2026-0002,2026-06-14,ops.manager,director,2026-07-01,POSITION,UPDATE,POS-1001,position_title,Sr. Data Engineer,Staff Data Engineer,Promotion
```

### 9.3 Entity Type Schemas

→ [[#9. Entity Management System]]

**POSITION**: position_id (VARCHAR), position_title (VARCHAR), department_id (VARCHAR), cost_center (VARCHAR), budgeted_salary_band (VARCHAR), is_manager_position (BOOLEAN), manager_employee_id (VARCHAR)

**EMPLOYEE**: employee_id (VARCHAR), employee_name (VARCHAR), employee_type (VARCHAR), hire_date (DATE), termination_date (DATE), department_id (VARCHAR)

**DEPARTMENT**: department_id (VARCHAR), department_name (VARCHAR), division (VARCHAR), cost_center_lead (VARCHAR)

### 9.4 Operation Semantics

→ [[#9. Entity Management System]]

- **CREATE**: Generate SK via MD5, insert with `effective_start_date = effective_date`, `is_current = TRUE`
- **UPDATE**: Close current row (`effective_end_date = effective_date - 1 day`, `is_current = FALSE`), insert new row with updated fields
- **DEACTIVATE**: Set `effective_end_date = effective_date`, `is_current = FALSE`. Do NOT delete.
- **REACTIVATE**: Insert new row with `effective_start_date = effective_date`, new SK

### 9.5 Dagster Sensor

→ [[#9. Entity Management System]]

**src/chronos_seat/defs/ingestion/rawgen/entity_request_sensor.py** — Entity request sensor — watches `data/entity_requests/inbox/` for new CSV files, validates them, and triggers CRUD processing.

```python
"""Dagster sensor — watches entity_requests/inbox/ for new files."""

from dagster import sensor, RunRequest, DefaultSensorStatus
from pathlib import Path
import polars as pl

ENTITY_INBOX = Path("data/entity_requests/inbox")
ENTITY_APPROVED = Path("data/entity_requests/approved")
ENTITY_REJECTED = Path("data/entity_requests/rejected")


def validate_entity_request(file_path: Path) -> tuple[bool, list[str]]:
    errors = []
    try:
        df = pl.read_csv(file_path)
    except Exception as e:
        return False, [f"Cannot read file: {e}"]

    for col in ["request_id", "request_date", "requested_by", "effective_date", "entity_type", "operation"]:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    if errors:
        return False, errors

    valid_types = {"POSITION", "EMPLOYEE", "DEPARTMENT"}
    if set(df["entity_type"].to_list()) - valid_types:
        errors.append(f"Invalid entity_type")
    valid_ops = {"CREATE", "UPDATE", "DEACTIVATE", "REACTIVATE"}
    if set(df["operation"].to_list()) - valid_ops:
        errors.append(f"Invalid operation")

    return len(errors) == 0, errors


@sensor(
    name="entity_request_sensor",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def entity_request_sensor(context):
    for p in [ENTITY_INBOX, ENTITY_APPROVED, ENTITY_REJECTED]:
        p.mkdir(parents=True, exist_ok=True)

    for file_path in ENTITY_INBOX.glob("*.csv"):
        is_valid, errors = validate_entity_request(file_path)
        dest = ENTITY_APPROVED / file_path.name if is_valid else ENTITY_REJECTED / file_path.name
        file_path.rename(dest)
        if is_valid:
            yield RunRequest(run_key=f"er_{file_path.stem}")
        else:
            context.log.warning(f"Rejected {file_path.name}: {errors}")
```

### 9.6 Entity Processing Asset (Full CRUD)

→ [[#9. Entity Management System]]

**src/chronos_seat/defs/ingestion/rawgen/entity_request_assets.py** — Entity request processing assets — full CRUD operations (CREATE, UPDATE, DEACTIVATE, REACTIVATE) for POSITION, EMPLOYEE, DEPARTMENT entities.

```python
"""Process approved entity requests — full SCD Type 2 CRUD for all entity types."""

from dagster import asset, AssetExecutionContext
import polars as pl
import clickhouse_connect
from pathlib import Path
from hashlib import md5


def _get_clickhouse():
    conn = clickhouse_connect.get_client(host="localhost", port=9000, user="default", password="", database="chronos")
    return conn


def _generate_sk(natural_key: str, effective_date: str) -> str:
    return md5(f"{natural_key}-{effective_date}".encode()).hexdigest()


def _apply_position_change(df: pl.DataFrame, operation: str, entity_id: str, effective_date: str):
    """Apply SCD Type 2 change to dim_position."""
    conn = _get_clickhouse()

    if operation == "CREATE":
        fields = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
        sk = _generate_sk(fields["position_id"], effective_date)
        conn.execute("""
            INSERT INTO main.dim_position
                (position_sk, position_id, position_title, department_id, cost_center,
                 is_manager_position, effective_start_date, effective_end_date,
                 is_current, source_system, inserted_at, _loaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', TRUE, 'ENTITY_MGMT', now(), now())
        """, [
            sk, fields["position_id"], fields.get("position_title", ""),
            fields.get("department_id", ""), fields.get("cost_center", ""),
            str(fields.get("is_manager_position", "FALSE")).lower() == "true",
            effective_date
        ])

    elif operation == "UPDATE":
        conn.execute("""
            UPDATE main.dim_position
            SET effective_end_date = ?::date - INTERVAL 1 DAY, is_current = FALSE
            WHERE position_id = ? AND is_current = TRUE
        """, [effective_date, entity_id])
        current = conn.execute("""
            SELECT * FROM main.dim_position
            WHERE position_id = ? AND is_current = FALSE
            ORDER BY effective_end_date DESC LIMIT 1
        """, [entity_id]).fetchone()
        if current:
            updates = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
            new_sk = _generate_sk(entity_id, effective_date)
            conn.execute("""
                INSERT INTO main.dim_position
                    (position_sk, position_id, position_title, department_id, cost_center,
                     is_manager_position, effective_start_date, effective_end_date,
                     is_current, source_system, inserted_at, _loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', TRUE, 'ENTITY_MGMT', now(), now())
            """, [
                new_sk, entity_id,
                updates.get("position_title", current[2]),
                updates.get("department_id", current[3]),
                updates.get("cost_center", current[4]),
                str(updates.get("is_manager_position", str(current[5]))).lower() == "true",
                effective_date
            ])

    elif operation == "DEACTIVATE":
        conn.execute("""
            UPDATE main.dim_position
            SET effective_end_date = ?, is_current = FALSE
            WHERE position_id = ? AND is_current = TRUE
        """, [effective_date, entity_id])

    elif operation == "REACTIVATE":
        current = conn.execute("""
            SELECT * FROM main.dim_position
            WHERE position_id = ? AND is_current = FALSE
            ORDER BY effective_end_date DESC LIMIT 1
        """, [entity_id]).fetchone()
        if current:
            new_sk = _generate_sk(entity_id, effective_date)
            conn.execute("""
                INSERT INTO main.dim_position
                    (position_sk, position_id, position_title, department_id, cost_center,
                     is_manager_position, effective_start_date, effective_end_date,
                     is_current, source_system, inserted_at, _loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', TRUE, 'ENTITY_MGMT', now(), now())
            """, [new_sk, entity_id, current[2], current[3], current[4], current[5], effective_date])
    conn.close()


def _apply_employee_change(df: pl.DataFrame, operation: str, entity_id: str, effective_date: str):
    """Apply SCD Type 2 change to dim_employee."""
    conn = _get_clickhouse()

    if operation == "CREATE":
        fields = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
        sk = _generate_sk(fields["employee_id"], effective_date)
        conn.execute("""
            INSERT INTO main.dim_employee
                (employee_sk, employee_id, employee_name, employee_type, hire_date,
                 termination_date, is_current, effective_start_date, effective_end_date,
                 source_system, inserted_at, _loaded_at)
            VALUES (?, ?, ?, ?, ?, NULL, TRUE, ?, '9999-12-31', 'ENTITY_MGMT', now(), now())
        """, [
            sk, fields["employee_id"], fields.get("employee_name", ""),
            fields.get("employee_type", "FULL-TIME"),
            fields.get("hire_date", effective_date),
            effective_date
        ])

    elif operation == "UPDATE":
        conn.execute("""
            UPDATE main.dim_employee
            SET effective_end_date = ?::date - INTERVAL 1 DAY, is_current = FALSE
            WHERE employee_id = ? AND is_current = TRUE
        """, [effective_date, entity_id])
        current = conn.execute("""
            SELECT * FROM main.dim_employee
            WHERE employee_id = ? AND is_current = FALSE
            ORDER BY effective_end_date DESC LIMIT 1
        """, [entity_id]).fetchone()
        if current:
            updates = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
            new_sk = _generate_sk(entity_id, effective_date)
            conn.execute("""
                INSERT INTO main.dim_employee
                    (employee_sk, employee_id, employee_name, employee_type, hire_date,
                     termination_date, is_current, effective_start_date, effective_end_date,
                     source_system, inserted_at, _loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, '9999-12-31', 'ENTITY_MGMT', now(), now())
            """, [
                new_sk, entity_id,
                updates.get("employee_name", current[2]),
                updates.get("employee_type", current[3]),
                updates.get("hire_date", current[4]),
                updates.get("termination_date", current[5]),
                effective_date
            ])

    elif operation == "DEACTIVATE":
        conn.execute("""
            UPDATE main.dim_employee
            SET effective_end_date = ?, is_current = FALSE
            WHERE employee_id = ? AND is_current = TRUE
        """, [effective_date, entity_id])

    elif operation == "REACTIVATE":
        current = conn.execute("""
            SELECT * FROM main.dim_employee
            WHERE employee_id = ? AND is_current = FALSE
            ORDER BY effective_end_date DESC LIMIT 1
        """, [entity_id]).fetchone()
        if current:
            new_sk = _generate_sk(entity_id, effective_date)
            conn.execute("""
                INSERT INTO main.dim_employee
                    (employee_sk, employee_id, employee_name, employee_type, hire_date,
                     termination_date, is_current, effective_start_date, effective_end_date,
                     source_system, inserted_at, _loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, '9999-12-31', 'ENTITY_MGMT', now(), now())
            """, [new_sk, entity_id, current[2], current[3], current[4], current[5], effective_date])
    conn.close()


def _apply_department_change(df: pl.DataFrame, operation: str, entity_id: str, effective_date: str):
    """Apply change to dim_department."""
    conn = _get_clickhouse()

    if operation == "CREATE":
        fields = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
        sk = fields["department_id"]
        conn.execute("""
            INSERT INTO main.dim_department
                (department_sk, department_id, department_name, division, cost_center_lead,
                 is_active, _loaded_at)
            VALUES (?, ?, ?, ?, ?, TRUE, now())
        """, [
            sk, fields["department_id"], fields.get("department_name", ""),
            fields.get("division", ""), fields.get("cost_center_lead", "")
        ])

    elif operation == "UPDATE":
        updates = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
        set_clauses = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [entity_id]
        conn.execute(f"""
            UPDATE main.dim_department
            SET {set_clauses}, _loaded_at = now()
            WHERE department_id = ?
        """, values)

    elif operation == "DEACTIVATE":
        conn.execute("""
            UPDATE main.dim_department SET is_active = FALSE, _loaded_at = now()
            WHERE department_id = ?
        """, [entity_id])

    elif operation == "REACTIVATE":
        conn.execute("""
            UPDATE main.dim_department SET is_active = TRUE, _loaded_at = now()
            WHERE department_id = ?
        """, [entity_id])
    conn.close()


@asset(group_name="entity_management")
def apply_entity_request(context: AssetExecutionContext):
    """Process approved entity requests from inbox."""
    approved_dir = Path("data/entity_requests/approved")
    archive_dir = Path("data/entity_requests/archive")
    archive_dir.mkdir(parents=True, exist_ok=True)

    files = list(approved_dir.glob("*.csv"))
    if not files:
        context.log.info("No approved entity requests to process")
        return

    for file_path in files:
        df = pl.read_csv(file_path)
        entity_type = df["entity_type"][0]
        operation = df["operation"][0]
        entity_id = df["entity_id"][0]
        effective_date = df["effective_date"][0]

        if entity_type == "POSITION":
            _apply_position_change(df, operation, entity_id, effective_date)
        elif entity_type == "EMPLOYEE":
            _apply_employee_change(df, operation, entity_id, effective_date)
        elif entity_type == "DEPARTMENT":
            _apply_department_change(df, operation, entity_id, effective_date)

        archive_path = archive_dir / file_path.name
        file_path.rename(archive_path)
        context.log.info(f"Processed: {file_path.name}")
```


Commit the Entity Management System:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add entity management system"
git push origin main


--Update dsrc/chronos_seat/definitions.pyns. to include the entity request assets and sensor:


"""Root Dagster definitions — merges all assets, resources, and sensors."""

from dagster import Definitions, load_assets_from_modules
from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import dbt_models  # @dbt_assets from DbtProject
from chronos_seat.defs.transformation.dbt.resources import dbt_resource
from chronos_seat.defs.ingestion.rawgen.sensors import change_request_sensor
from chronos_seat.defs.ingestion.rawgen.sensors import entity_request_sensor
from chronos_seat.defs.ingestion.rawgen import entity_request_assets

all_assets = [
    dbt_models,                                 # dbt models as Dagster assets (bronze → silver → gold)
    *load_assets_from_modules([entity_request_assets]),  # Entity CRUD (SCD Type 2)
]

all_sensors = [
    change_request_sensor,  # Watches change_requests/inbox/ for new CSV/Excel files
    entity_request_sensor,  # Watches entity_requests/inbox/ for new CSV/Excel files
]

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
    resources={
        "clickhouse": clickhouse_resource,
        "dbt": dbt_resource,
    },
)



``
10. Web Portal

→ l

Table of Contentste

s]]
10.1 Scaffold

→ d

10. Web Portalor


cd ~/workspace/projects/chronos-seat
npx create-next-app@latest portal --typescript --tailwind --eslint --app --src-dir --import-alias "@/*" --use-npm
cd portal
npm install @tanstack/react-table recharts zustand


```
10.2 Local Development

→ t

10. Web Portalor


# Terminal 1: Dagster
cd ~/workspace/projects/chronos-seat
uv run dg dev -h 0.0.0.0 -p 2320

# Terminal 2: Rill
rill start  ./rill_dashboard --port 2321

# Terminal 3: Portal
cd ~/workspace/projects/chronos-seat/portal
npm run dev
# → http://localhost:2319


Required environment variables for the portal (create eportal/.env.locall):


DAGSTER_URL=http://localhost:2320
RILL_URL=http://localhost:2321
CLICKHOUSE_HOST=http://localhost:8123


```
10.3 Key Components

→ s

10. Web Portalor

l]components/layout/Navbar.tsxr. — Portal navbar — top navigation bar for the web portal with links to dashboards, entities, and change requests.


"use client";
import Link from "next/link";

const NAV_ITEMS = [
  { href: "/", label: "Dashboard" },
  { href: "/entities", label: "Entities" },
  { href: "/changes", label: "Changes" },
  { href: "/admin", label: "Admin" },
  { href: "/integrations", label: "Integrations" },
];

export default function Navbar() {
  return (
    <nav className="bg-slate-900 text-white px-6 py-3 flex items-center gap-6">
      <span className="font-bold text-lg">ChronosSeat</span>
      {NAV_ITEMS.map((item) => (
        <Link key={item.href} href={item.href} className="hover:text-blue-400">
          {item.label}
        </Link>
      ))}
    </nav>
  );
}


``components/dashboard/DashboardEmbed.tsxd. — Dashboard embed component — iframe wrapper for embedding Rill dashboards in the portal.


export default function DashboardEmbed({ src, title }: { src: string; title: string }) {
  // src should be a full URL like http://localhost:2321 or a Rill Cloud embed URL
  return (
    <div className="w-full">
      <h2 className="text-lg font-semibold mb-2">{title}</h2>
      <iframe
        src={src}
        className="w-full h-[600px] border-0 rounded-lg"
        title={title}
      />
    </div>
  );
}


``components/entities/EntityTable.tsxe. — Entity table component — renders SCD Type 2 entity data with history, filtering, and pagination.


"use client";
import { useState, useMemo } from "react";
import {
  useReactTable, getCoreRowModel, getFilteredRowModel,
  getPaginationRowModel, flexRender, createColumnHelper,
} from "@tanstack/react-table";
import Link from "next/link";

type EntityRecord = Record<string, any>;

export default function EntityTable({
  data, entityType, idField = "position_id", titleField = "position_title",
}: {
  data: EntityRecord[];
  entityType: string;
  idField?: string;
  titleField?: string;
}) {
  const [globalFilter, setGlobalFilter] = useState("");

  const columns = useMemo(() => {
    const h = createColumnHelper<EntityRecord>();
    return [
      h.accessor(idField, { header: "ID", cell: (info: any) => (
        <Link href={`/entities/${entityType}/${info.getValue()}`} className="text-blue-400 hover:underline">
          {info.getValue()}
        </Link>
      )}),
      h.accessor(titleField, { header: "Title" }),
      h.accessor("department_id", { header: "Department" }),
      h.accessor("is_current", { header: "Status", cell: (info: any) => (
        <span className={info.getValue() ? "text-green-400" : "text-gray-500"}>
          {info.getValue() ? "● Active" : "○ Inactive"}
        </span>
      )}),
    ];
  }, [entityType, idField, titleField]);

  const table = useReactTable({
    data, columns,
    state: { globalFilter },
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  });

  return (
    <div>
      <input
        value={globalFilter}
        onChange={(e) => setGlobalFilter(e.target.value)}
        placeholder="Search..."
        className="mb-4 px-3 py-2 bg-slate-800 text-white rounded w-full max-w-md"
      />
      <table className="w-full text-sm">
        <thead>
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id} className="border-b border-slate-700">
              {hg.headers.map((h) => (
                <th key={h.id} className="text-left py-2 px-3">
                  {flexRender(h.column.columnDef.header, h.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id} className="border-b border-slate-800 hover:bg-slate-800/50">
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id} className="py-2 px-3">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


```
10.4 API Routes

→ s

10. Web Portalor

l]app/api/entities/route.tste — Entity API route — Next.js API endpoint for CRUD operations on entities. Reads/writes to ClickHouse gold schema via HTTP interface.


import { NextRequest, NextResponse } from "next/server";

const CLICKHOUSE_HOST = process.env.CLICKHOUSE_HOST || "http://localhost:8123";

async function queryClickhouse(sql: string, params: Record<string, string> = {}) {
  const formatted = Object.entries(params).reduce(
    (s, [k, v]) => s.replace(`{${k}}`, v),
    sql
  );
  const res = await fetch(`${CLICKHOUSE_HOST}/?query=${encodeURIComponent(formatted)}`, {
    method: "POST",
    headers: { "Content-Type": "text/plain" },
  });
  return res.json();
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type") || "position";
  const search = searchParams.get("search") || "";
  const status = searchParams.get("status") || "current";

  const table = `dim_${type.toLowerCase()}`;
  const isCurrent = status === "current" ? 1 : 0;

  let sql = `SELECT * FROM chronos.{table} WHERE is_current = {isCurrent:UInt8}`;
  const params: Record<string, string> = { table, isCurrent: String(isCurrent) };

  if (search) {
    sql += ` AND (position_title LIKE {search:String} OR position_id LIKE {search2:String})`;
    params.search = `%${search}%`;
    params.search2 = `%${search}%`;
  }

  sql += " ORDER BY position_id FORMAT JSON";

  try {
    const data = await queryClickhouse(sql, params);
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}


    conn.all(sql, ...params, (err: any, rows: any) => {
      conn.close();
      if (err) {
        resolve(NextResponse.json({ error: err.message }, { status: 500 }));
      } else {
        resolve(NextResponse.json(rows));
      }
    });
  });
}


### 10.5 Portal Dockerfile

→ [[#10. Web Portal]]

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build
EXPOSE 2319
CMD ["npm", "start"]



Commit the Web Portal:


cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add web portal"
git push origin main




---11. Docker Deployment

→ t

Table of Contentste

s]]
11.1 Dockerfile (Dagster Host Processes)

→ )

11. Docker Deploymentym

> This image is used by both bdagster-webservere and  dagster-daemona. It contains
> Dagster packages + configuration, but no user code (user code is baked in via
> iCOPY src/  below since this is a single-container deployment).


FROM python:3.13-slim

RUN groupadd -r appuser && useradd -r -g appuser appuser
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
RUN uv sync --frozen --no-dev

# Copy user code
COPY src/ ./src
COPY dbt_project/ ./dbt_project

# Copy Dagster config into DAGSTER_HOME
# workspace.yaml tells Dagster where to find code (src.chronos_seat)
# dagster.yaml configures the Dagster instance (run launcher, coordinator, etc.)
RUN mkdir -p /app/dagster_home
COPY workspace.yaml dagster.yaml /app/dagster_home/

USER appuser
EXPOSE 2320


```
11.2 docker-compose.yml

→ l

11. Docker Deploymentym

> Three long-running containers: webserver, daemon, and Rill. Each runs in its own
> container from the same Docker image. The webserver and daemon are independent
> host processes — they do NOT depend on each other.


version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"
    volumes:
      - clickhouse-data:/var/lib/clickhouse
    environment:
      - CLICKHOUSE_DB=chronos
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=""
    networks:
      - chronos-net
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "clickhouse-client", "--host", "localhost", "--port", "9000", "--query", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 15s

  dagster-webserver:
    build: .
    entrypoint: ["dagster-webserver"]
    command: ["-h", "0.0.0.0", "-p", "2320"]
    volumes:
      - chronos-data:/data
    ports:
      - "2320:2320"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:2320/server_info"]
      interval: 15s
      timeout: 10s
      retries: 5
      start_period: 30s
    environment:
      - DAGSTER_HOME=/app/dagster_home
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PORT=9000
    networks:
      - chronos-net
    depends_on:
      clickhouse:
        condition: service_healthy
    restart: unless-stopped

  dagster-daemon:
    build: .
    entrypoint: ["dagster-daemon"]
    command: ["run"]
    volumes:
      - chronos-data:/data
    environment:
      - DAGSTER_HOME=/app/dagster_home
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PORT=9000
    networks:
      - chronos-net
    depends_on:
      clickhouse:
        condition: service_healthy
    restart: on-failure

  rill:
    image: ghcr.io/rilldata/rill:latest
    volumes:
      - chronos-data:/data:ro
    ports:
      - "2321:2321"
    command: start /data/rill_dashboard --port 2321
    depends_on:
      dagster-webserver:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
    networks:
      - chronos-net
    restart: unless-stopped

  portal:
    build: ./portal
    ports:
      - "2319:2319"
    environment:
      - DAGSTER_URL=http://dagster-webserver:2320
      - RILL_URL=http://rill:2321
      - CLICKHOUSE_HOST=http://clickhouse:8123
    volumes:
      - chronos-data:/data:ro
    depends_on:
      - dagster-webserver
      - rill
      - clickhouse
    networks:
      - chronos-net
    restart: unless-stopped

volumes:
  chronos-data:
  clickhouse-data:

networks:
  chronos-net:
    driver: bridge
    name: chronos-net

    name: chronos-net


> **Key differences from local dev**:
> - `DAGSTER_HOME=/app/dagster_home` (not `./dagster_home` — containers use absolute paths)
> - `dagster.yaml` and `workspace.yaml` are baked into the image at build time
> - `dagster-daemon` has no `depends_on` for the webserver — these are independent
>   host processes that run in parallel (matches the Dagster deployment architecture)


Commit the Docker Deployment:

```bash
cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add docker deployment"
git push origin main




---12. Testing

→ g

Table of Contentste

s]]
12.1 Makefile

→ e

12. Testingst


.PHONY: help setup ingest transform validate pipeline test lint format clean docker-up docker-down

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup:
	uv sync
	mkdir -p data/change_requests/{inbox,approved,rejected,processing,archive}
	mkdir -p data/entity_requests/{inbox,approved,rejected,processing,archive}
	mkdir -p dagster_home        # Dagster's working directory: stores run history, schedules, sensor state
	touch data/change_requests/{inbox,approved,rejected,processing,archive}/.gitkeep
	touch data/entity_requests/{inbox,approved,rejected,processing,archive}/.gitkeep
	# Write dagster.yaml inside dagster_home/ — Dagster reads $DAGSTER_HOME/dagster.yaml at startup
	printf 'run_launcher:\n  module: dagster\n  class: DefaultRunLauncher\n\nrun_coordinator:\n  module: dagster\n  class: QueuedRunCoordinator\n\nschedules:\n  use_threads: true\n  num_workers: 4\n\nsensors:\n  use_threads: true\n  num_workers: 4\n\ntelemetry:\n  enabled: false\n' > dagster_home/dagster.yaml
	cd dbt_project && uv run dbt deps
	pre-commit install

ingest:
	uv run dg dev -h 0.0.0.0 -p 2320  # Starts webserver + daemon (required for sensors)

transform:
	cd dbt_project && uv run dbt seed && uv run dbt build

validate:
	cd dbt_project && uv run dbt test

pipeline: transform validate

test: validate
	uv run pytest tests/ -v

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

clean:
	rm -rf dbt_project/target dbt_project/logs dagster_home
	rm -rf __pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down


```
12.2 Test Files

→ s

12. Testingst

g]tests/test_ingestion.pyon — Ingestion tests — pytest tests for file-based ingestion assets (file reading, bronze table writes, validation).


"""Tests for ingestion pipeline."""

import polars as pl
from pathlib import Path
import pytest


def test_raw_directory_structure():
    """Bronze tables should exist in ClickHouse."""
    import clickhouse_connect
    conn = clickhouse_connect.get_client(host="localhost", port=9000, user="default", password="", database="chronos")
    tables = conn.execute("SELECT name FROM system.tables WHERE database = 'chronos'").fetchall()
    table_names = [t[0] for t in tables]
    assert "bronze_erp_roster" in table_names
    assert "bronze_hr_allocations" in table_names
    assert "bronze_contractor_tracking" in table_names


def test_no_duplicate_employee_ids():
    """Each employee_id should appear once in the roster."""
    import clickhouse_connect
    conn = clickhouse_connect.get_client(host="localhost", port=9000, user="default", password="", database="chronos")
    result = conn.execute("SELECT count() as total, count(DISTINCT employee_id) as unique_count FROM chronos.bronze_erp_roster").fetchone()
    assert result[0] == result[1]


``tests/test_scd2_constraints.pyts — SCD Type 2 constraint tests — validates no overlapping date ranges, no duplicate current rows, FK integrity.


"""Tests for SCD Type 2 constraints."""

import clickhouse_connect
import pytest


def _get_conn():
    return clickhouse_connect.get_client(host="localhost", port=9000, user="default", password="", database="chronos")


def test_no_duplicate_current_positions():
    conn = _get_conn()
    result = conn.execute("""
        SELECT position_id, count() as cnt
        FROM chronos.dim_position WHERE is_current = 1
        GROUP BY position_id HAVING cnt > 1
    """).fetchall()
    assert len(result) == 0


def test_position_sk_uniqueness():
    conn = _get_conn()
    result = conn.execute("""
        SELECT position_sk, count() as cnt
        FROM chronos.dim_position
        GROUP BY position_sk HAVING cnt > 1
    """).fetchall()
    assert len(result) == 0


def test_effective_date_order():
    conn = _get_conn()
    result = conn.execute("""
        SELECT position_id FROM chronos.dim_position
        WHERE effective_start_date > effective_end_date
    """).fetchall()
    assert len(result) == 0


def test_employee_sk_uniqueness():
    conn = _get_conn()
    result = conn.execute("""
        SELECT employee_sk, count() as cnt
        FROM chronos.dim_employee
        GROUP BY employee_sk HAVING cnt > 1
    """).fetchall()
    assert len(result) == 0


``tests/test_dbt_transforms.pyms — dbt transform tests — validates gold table existence, row counts, and schema correctness after dbt runs.


"""Tests for dbt transformations."""

import clickhouse_connect
import pytest


def _get_conn():
    return clickhouse_connect.get_client(host="localhost", port=9000, user="default", password="", database="chronos")


def test_dim_position_exists():
    conn = _get_conn()
    result = conn.execute("SELECT count() FROM chronos.dim_position").fetchone()
    assert result[0] > 0


def test_dim_employee_exists():
    conn = _get_conn()
    result = conn.execute("SELECT count() FROM chronos.dim_employee").fetchone()
    assert result[0] > 0


def test_fact_event_exists():
    conn = _get_conn()
    result = conn.execute("SELECT count() FROM chronos.fact_position_occupancy_event").fetchone()
    assert result[0] > 0


def test_bridge_table_exists():
    conn = _get_conn()
    result = conn.execute("SELECT COUNT(*) FROM main.bridge_position_occupancy").fetchone()
    assert result[0] > 0




Commit the Testing:


cd ~/workspace/projects/chronos-seat
git add .
git commit -m "feat: add testing"
git push origin main




---13. Network Access

→ s

Table of Contentste

s]]
13.1 Port Registry

→ y

13. Network Accesscc


y |
13.2 Local Network (LAN)

→ )

13. Network Accesscc


uv run dg dev -h 0.0.0.0 -p 2320
rill start  ./rill_dashboard --port 2321 --host 0.0.0.0
# Others access via: http://<your-ip>:2320 and http://<your-ip>:2321


```
13.3 Reverse Proxy (nginx)

→ )

13. Network Accesscc

s]nginx/nginx.conf.c — Nginx reverse proxy config — routes traffic to Dagster (2319), Rill (8080), and Portal (3000) on a single port.


events { worker_connections 1024; }

http {
    server {
        listen 80;
        server_name _;
        return 301 https://$host$request_uri;
    }
    server {
        listen 443 ssl;
        server_name _;
        ssl_certificate     /etc/nginx/certs/fullchain.pem;
        ssl_certificate_key /etc/nginx/certs/privkey.pem;

        location /dagster/ {
            proxy_pass http://dagster-webserver:2320/;
            proxy_set_header Host $host;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
        location / {
            proxy_pass http://rill:2321;
            proxy_set_header Host $host;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
    }
}


Generate self-signed certs:


mkdir -p nginx/certs
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/certs/privkey.pem \
  -out nginx/certs/fullchain.pem \
  -subj "/CN=chronos-seat.local"



---14. Scaling Path

→ h

Table of Contentste

s]]
Phase 1: Local (Current)

→ )

14. Scaling Path P

- ClickHouse (Docker) + Local Rill + Mock data
- Multi-user on LAN, zero cost

ost
Phase 2: Multi-Team

→ m

14. Scaling Path P

- ClickHouse (Docker or cloud) + Rill Cloud
- Migration: update dprofiles.ymls host/port, set credentials

als
Phase 3: Enterprise

→ e

14. Scaling Path P

- ClickHouse cluster + Dagster+ + Rill Cloud
- Migration: update dbt  profiles.ymls target, update Rill sources


---15. Quick Reference — Run Everything Locally

→ y

Table of Contentste


# 1. First-time setup
cd ~/workspace/projects/chronos-seat
make setup

# 2. Materialize mock data (terminal 1)
uv run dg dev -h 0.0.0.0 -p 2320
# In Dagster UI → Assets → select all → Materialize

# 3. Run dbt pipeline
make pipeline

# 4. Start Rill (terminal 2)
rill start  ./rill_dashboard --port 2321
# → http://localhost:2321

# 5. Start portal (terminal 3)
cd portal && npm run dev
# → http://localhost:2319

# 6. Run tests
make test

# 7. Verify
curl http://localhost:2320/server_info    # Dagster
curl http://localhost:2321/health         # Rill
curl http://localhost:2319                # Portal


```
End-to-End Data Flow

→ w

15. Quick Reference — Run Everything Locallyca


[Mock Data Assets] → bronze.* (ClickHouse)
        ↓
[dbt seed] → dim_change_type, dim_change_reason, dim_department
        ↓
[dbt run] → dim_date (date spine model)
        ↓
[dbt build] → dim_position, dim_employee, fact_position_occupancy_event, bridge_position_occupancy
        ↓
[Rill] → reads from ClickHouse (chronos database) → dashboards
        ↓
[Portal] → reads from ClickHouse HTTP API → entity browser + change requests


```
Demo Script (5 minutes)

→ )

15. Quick Reference — Run Everything Locallyca

16. Open Ohttp://localhost:2319: → show Rill dashboard with occupancy trends
17. Click "Entities" → search for POS-1001 → click row → show SCD history
18. Click "Changes" → "New Change" → submit a NEW_HIRE form
19. Show the change appear in the pending list
20. Open Ohttp://localhost:2320: → show Dagster asset graph + run history


---

## 17. v3 Fixes — Critical Patches

→ [[#Table of Contents]]

> Applied 2026-06-15 after v2 eval. These patches fix 7 issues found in v2.

### 17.1 Security: SQL Injection in Entity API Route

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `portal/app/api/entities/route.ts`

The `type` query parameter was interpolated directly into SQL. Whitelist allowed values:

```typescript
export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type") || "position";
  const search = searchParams.get("search") || "";
  const status = searchParams.get("status") || "current";

  // WHITELIST allowed entity types to prevent SQL injection
  const ALLOWED_TYPES = ["position", "employee", "department"];
  if (!ALLOWED_TYPES.includes(type.toLowerCase())) {
    return NextResponse.json({ error: `Invalid type. Allowed: ${ALLOWED_TYPES.join(", ")}` }, { status: 400 });
  }

// Use ClickHouse HTTP API instead of DuckDB (file-level lock prevented concurrent access)
  const res = await fetch(`${CLICKHOUSE_HOST}/?query=${encodeURIComponent(sql)}`, {
    method: "POST",
    headers: { "Content-Type": "text/plain" },
  });
  const data = await res.json();
  return NextResponse.json(data);

    conn.all(sql, ...params, (err: any, rows: any) => {
      conn.close();
      if (err) {
        resolve(NextResponse.json({ error: err.message }, { status: 500 }));
      } else {
        resolve(NextResponse.json(rows));
      }
    });
  });
}
```

### 17.2 Security: SQL Injection in Department UPDATE

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `src/chronos_seat/defs/ingestion/rawgen/entity_request_assets.py`

The `_apply_department_change` function used f-string interpolation for column names. Whitelist allowed fields:

```python
def _apply_department_change(df: pl.DataFrame, operation: str, entity_id: str, effective_date: str):
    """Apply change to dim_department."""
    # WHITELIST allowed fields to prevent SQL injection
    ALLOWED_DEPT_FIELDS = {"department_name", "division", "cost_center_lead", "department_id"}

    conn = _get_clickhouse()

    if operation == "CREATE":
        fields = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
        sk = fields["department_id"]
        conn.execute("""
            INSERT INTO main.dim_department
                (department_sk, department_id, department_name, division, cost_center_lead,
                 is_active, _loaded_at)
            VALUES (?, ?, ?, ?, ?, TRUE, now())
        """, [
            sk, fields["department_id"], fields.get("department_name", ""),
            fields.get("division", ""), fields.get("cost_center_lead", "")
        ])

    elif operation == "UPDATE":
        updates = {row["field_name"]: row["new_value"] for row in df.iter_rows(named=True)}
        # Only allow whitelisted fields
        safe_updates = {k: v for k, v in updates.items() if k in ALLOWED_DEPT_FIELDS}
        if not safe_updates:
            context.log.warning(f"No valid fields to update for {entity_id}")
            conn.close()
            return
        set_clauses = ", ".join(f"{k} = ?" for k in safe_updates.keys())
        values = list(safe_updates.values()) + [entity_id]
        conn.execute(f"""
            UPDATE main.dim_department
            SET {set_clauses}, _loaded_at = now()
            WHERE department_id = ?
        """, values)

    elif operation == "DEACTIVATE":
        conn.execute("""
            UPDATE main.dim_department SET is_active = FALSE, _loaded_at = now()
            WHERE department_id = ?
        """, [entity_id])

    elif operation == "REACTIVATE":
        conn.execute("""
            UPDATE main.dim_department SET is_active = TRUE, _loaded_at = now()
            WHERE department_id = ?
        """, [entity_id])
    conn.close()
```

### 17.3 Missing `silver_contractor_tracking` Asset

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `src/chronos_seat/defs/transformation/adhoc/assets.py`

> **Note:** In the current architecture, `bronze_contractor_tracking` is a dbt model (§4.2), not a Python Dagster asset. The silver transform should also be a dbt model. Create `dbt_project/models/silver/silver_contractor_tracking.sql` instead of a Python asset:

```sql
{{
    config(
        materialized='table',
        schema='silver'
    )
}}
SELECT
    contractor_id AS employee_id,
    contractor_name AS employee_name,
    position_id,
    start_date AS assignment_start,
    end_date AS assignment_end,
    'CONTRACTOR' AS employee_type
FROM {{ ref('bronze_contractor_tracking') }}
```

### 17.4 Missing `stg_contractor_tracking` dbt Model

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `dbt_project/models/staging/stg_contractor_tracking.sql`

```sql
{{ config(materialized='view', schema='silver') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'contractor_tracking') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(employee_id)) AS employee_id,
        INITCAP(TRIM(employee_name)) AS employee_name,
        UPPER(TRIM(position_id)) AS position_id,
        assignment_start,
        assignment_end,
        employee_type,
        current_timestamp AS _loaded_at
    FROM source
)

SELECT * FROM cleaned
```

### 17.5 Wire Sensors and Entity Assets into Definitions

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `src/chronos_seat/definitions.py`

The sensors and entity assets must be imported so Dagster discovers them. This is the final complete `definitions.py` — it includes everything built across all prior sections:

```python
"""Root Dagster definitions — merges all assets, resources, and sensors."""

from dagster import Definitions, load_assets_from_modules
from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import dbt_models  # @dbt_assets from DbtProject
from chronos_seat.defs.transformation.dbt.resources import dbt_resource
# Uncomment when you reach section 8 (change request sensor):
# from chronos_seat.defs.ingestion.rawgen.sensors import change_request_sensor
# Uncomment when you reach section 9 (entity request sensor):
# from chronos_seat.defs.ingestion.rawgen.sensors import entity_request_sensor

all_assets = [
    dbt_models,                                 # dbt models as Dagster assets (bronze → silver → gold)
    # Uncomment when you reach section 9 (entity request assets):
    # *load_assets_from_modules([entity_request_assets]),
]

all_sensors = [
    # Uncomment when you reach section 8:
    # change_request_sensor,
    # Uncomment when you reach section 9:
    # entity_request_sensor,
]

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors if all_sensors else None,
    resources={
        "clickhouse": clickhouse_resource,  # ClickhouseResource from dagster-clickhouse
        "dbt": dbt_resource,                # DbtCliResource — runs dbt commands from Dagster
    },
)
```

### 17.6 Rill Container Volume Fix

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `docker-compose.yml`

The Rill container needs the rill_dashboard files. Mount them from the host:

```yaml
  rill:
    image: ghcr.io/rilldata/rill:0.50.0
    volumes:
      - chronos-data:/data:ro
      - ./rill_dashboard:/data/rill_dashboard:ro
    ports:
      - "2321:2321"
    command: start /data/rill_dashboard --port 2321
    depends_on:
      dagster-webserver:
        condition: service_healthy
    networks:
      - chronos-net
    restart: unless-stopped
```

### 17.7 Fix CI Pipeline — Materialize Mock Data Before dbt

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `.github/workflows/ci.yml`

The CI must create mock data before running dbt. Replace the `test` job's pipeline step:

```yaml
      - name: Run pipeline
        run: |
          source $HOME/.local/bin/env
          # Create mock raw data (simulates Dagster materialization)
          cd dbt_project && uv run dbt seed && uv run dbt build
```

> **Note**: In CI without Dagster, the mock data assets won't be auto-generated. For a fully automated CI, either:
> (a) Add a pre-dbt step that generates the CSV/Parquet files directly, or
> (b) Run `uv run dagster asset materialize --select "*"` before dbt.
>
> Simplest approach for CI — load seed data and run dbt models:

```yaml
      - name: Load seeds and run dbt models for CI
        run: |
          source $HOME/.local/bin/env
          cd dbt_project && uv run dbt seed && uv run dbt build
```

### 17.8 Fix Makefile setup — Create All Data Directories

→ [[#17. v3 Fixes — Critical Patches]]

**File**: `Makefile`

The `setup` target creates all required directories. ClickHouse manages all Medallion layers (bronze, silver, gold) as schemas in the `chronos` database:

```makefile
setup:
	uv sync
	mkdir -p data/change_requests/{inbox,approved,rejected,processing,archive}
	mkdir -p data/entity_requests/{inbox,approved,rejected,processing,archive}
	touch data/change_requests/{inbox,approved,rejected,processing,archive}/.gitkeep
	touch data/entity_requests/{inbox,approved,rejected,processing,archive}/.gitkeep
	# Initialize ClickHouse database
	clickhouse-client --host localhost --port 9000 --user default --query "CREATE DATABASE IF NOT EXISTS chronos"
	cd dbt_project && uv run dbt deps
	pre-commit install
```
