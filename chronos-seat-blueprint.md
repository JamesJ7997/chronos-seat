# ChronosSeat Blueprint v4

> **Architecture blueprint for the ChronosSeat data platform.**
> **Version**: 4.0 — ClickHouse backend for concurrent multi-user access
> **Previous**: v3.0 (DuckLake single-catalog architecture)
> **Migration**: DuckLake → ClickHouse. DuckLake's file-level lock prevents concurrent access — Rill dashboard and change requests cannot share the same file. ClickHouse handles concurrent natively.
> **Last updated**: 2026-06-19

---

## 1. Architecture Overview

### 1.1 Design Principles

1. **File-first ingestion**: Production data arrives as files (CSV, Parquet, Excel) from SAP, SharePoint, and Excel. The system watches for new files and ingests them into ClickHouse bronze.
2. **Mock data as fallback**: Mock generators exist for development/demo but are not the production path.
3. **ClickHouse owns all layers**: Bronze, silver, and gold schemas live inside a single ClickHouse database. No flat-file staging directories.
4. **Medallion architecture**: Bronze (raw ingested) → Silver (cleaned/conformed) → Gold (dimensional models).
5. **Dagster orchestration**: Sensors watch for new files. Assets transform data. dbt handles SQL-based transformations.
6. **Incremental strategy**: Bronze tables use `CREATE OR REPLACE` (full refresh) since files represent complete snapshots. Silver and gold layers use incremental logic where appropriate (e.g., `fact_position_occupancy_event` is append-only, `dim_position`/`dim_employee` use SCD Type 2).

**File validation rules**:
| Check | Failure Action |
|-------|---------------|
| File readable (correct format) | Move to `failed/`, log error |
| File not empty (> 0 rows) | Move to `failed/`, log error |
| Required columns present | Move to `failed/`, log error |
| No duplicate primary keys | Move to `failed/`, log error |

### 1.2 Production Data Sources

| Source | Format | Content | Frequency | Owner |
|--------|--------|---------|-----------|-------|
| SAP / ERP | CSV export | Employee roster, positions, departments, cost centers | Daily/weekly batch | HR / IT |
| SharePoint | CSV or Excel | Contractor tracking, position assignments | Ad hoc | Operations |
| Excel | .xlsx | Cross-reference data, manual adjustments, joined tables | Ad hoc | Business users |

**Key insight**: Users cannot switch to direct API connections to these systems initially. File drop is the only viable ingestion method for the foreseeable future. The system must be designed around this constraint.

### 1.3 Ingestion Architecture

```
[SAP CSV export] ──┐
[SharePoint file] ──┼──→ data/inbox/ ──→ Dagster sensor ──→ bronze.* tables
[Excel file] ──────┘                      (file type routing)
                                                │
[Mock generators] ──→ (dev only) ───────────────┘
```

**File routing**: Files are placed in `data/inbox/` with a naming convention that indicates the source system and entity type:
- `erp_roster_YYYYMMDD.csv` — SAP ERP employee roster
- `sharepoint_tracking_YYYYMMDD.csv` — SharePoint contractor tracking
- `excel_crossref_YYYYMMDD.xlsx` — Excel cross-reference data

**Sensor behavior**: A single `file_watcher_sensor` scans `data/inbox/`, routes files to the appropriate ingestion asset based on filename pattern, and moves processed files to `data/archive/`.

### 1.4 End-to-End Data Flow

```
[File drop: data/inbox/]
        ↓
[Dagster file_watcher_sensor] → detects new file, routes by filename pattern
        ↓
[Ingestion assets] → read file (CSV/Parquet/Excel) → write to bronze.* tables in ClickHouse
        ↓
[Silver transforms (Dagster assets)] → bronze.* → silver.* (cleaning, standardization)
        ↓
[dbt staging models] → silver.* views (light validation, column selection)
        ↓
[dbt mart models] → gold.* tables (dim_position, dim_employee, fact tables, bridge)
        ↓
[Rill] → reads gold.* → dashboards
[Portal] → reads gold.* → entity browser + change requests
```

---

## 2. Project Structure

```
chronos-seat/
├── data/
│   ├── inbox/                          # Drop zone for incoming files
│   │   ├── erp_roster_20260618.csv     # SAP ERP export
│   │   ├── sharepoint_tracking_20260618.csv  # SharePoint export
│   │   └── excel_crossref_20260618.xlsx     # Excel cross-reference
│   ├── inbox/failed/                   # Files that failed validation
│   ├── archive/                        # Processed files (timestamped)
│   ├── change_requests/                # Change request workflow
│   │   ├── inbox/
│   │   ├── approved/
│   │   ├── rejected/
│   │   ├── processing/
│   │   └── archive/
│   └── entity_requests/                # Entity management workflow
│       ├── inbox/
│       ├── approved/
│       ├── rejected/
│       ├── processing/
│       └── archive/
├── dbt_project/
│   ├── macros/
│   │   └── generate_sk.sql          # Surrogate key generation macro
│   ├── models/
│   │   ├── staging/                    # stg_*.sql — light validation views
│   │   ├── intermediate/               # int_*.sql — business logic joins
│   │   └── marts/                      # dim_*.sql, fact_*.sql — gold layer
│   ├── seeds/                          # Reference data CSVs
│   └── tests/                          # Schema tests
├── rill_dashboard/                     # Rill BI dashboards
├── src/chronos_seat/
│   ├── definitions.py                  # Root Dagster Definitions
│   └── defs/
│       ├── ingestion/
│       │   └── rawgen/
│       │       ├── assets.py           # File-based ingestion + mock generators
│       │       ├── file_sensor.py      # Watches data/inbox/ for new files
│       │       ├── change_request_sensor.py
│       │       ├── entity_request_sensor.py
│       │       ├── entity_request_assets.py
│       │       └── resources.py        # ClickhouseResource
│       └── transformation/
│           ├── adhoc/
│           │   └── assets.py           # Bronze → Silver Python transforms
│           └── dbt/
│               ├── assets.py           # dbt asset integration
│               ├── project.py          # DbtProject configuration
│               └── resources.py        # DbtCliResource
├── scripts/
│   └── log-change.sh                   # Changelog helper (gitignored)
├── tests/
│   ├── test_ingestion.py               # Ingestion asset tests
│   ├── test_dbt_transforms.py          # dbt model tests
│   └── test_change_requests.py         # Change request workflow tests
├── dagster_home/                       # Dagster instance data
├── .gitignore
├── CHANGELOG.md
├── Makefile
├── pyproject.toml
└── docker-compose.yml
```

---

## 3. Ingestion Layer (Bronze)

### 3.1 File Watcher Sensor

**File**: `src/chronos_seat/defs/ingestion/rawgen/file_sensor.py`

Watches `data/inbox/` for new files. Routes to the appropriate ingestion asset based on filename pattern:

| Filename Pattern | Source System | Target Table |
|-----------------|---------------|--------------|
| `erp_roster_*.csv` | SAP/ERP | `bronze.erp_roster` |
| `sharepoint_tracking_*.csv` | SharePoint | `bronze.contractor_tracking` |
| `excel_crossref_*.xlsx` | Excel | `bronze.excel_cross_reference` |

**Behavior**:
1. Scan `data/inbox/` every 30 seconds
2. Match filename against known patterns
3. Validate file:
   - File is readable (correct format)
   - File is not empty (> 0 rows)
   - Required columns present (see table below)
   - No duplicate `request_id` values within file
4. On validation failure: move to `data/inbox/failed/`, log error, skip
5. On validation success: yield `RunRequest` with file path and target table as config
6. After successful processing: move file to `data/archive/` with timestamp prefix

### 3.2 Ingestion Assets

**File**: `src/chronos_seat/defs/ingestion/rawgen/assets.py`

Contains both production file-based ingestion assets AND mock data generators:

**Production assets** (file-based):
- `ingest_erp_roster` — Reads CSV from `data/inbox/erp_roster_*.csv` → `bronze.erp_roster`
- `ingest_contractor_tracking` — Reads CSV from `data/inbox/sharepoint_tracking_*.csv` → `bronze.contractor_tracking`
- `ingest_excel_crossref` — Reads Excel from `data/inbox/excel_crossref_*.xlsx` → `bronze.excel_cross_reference`

**Mock assets** (development only):
- `mock_erp_roster` — Generates synthetic ERP data → `bronze.erp_roster`
- `mock_hr_allocations` — Generates synthetic HR allocation data → `bronze.hr_allocations`
- `mock_contractor_tracking` — Generates synthetic contractor data → `bronze.contractor_tracking`

**Key difference from v2**: In v2, mock assets were the only ingestion path and they wrote directly to DuckLake (file-based, single-writer). In v3, production assets read from files and write to ClickHouse (client-server, concurrent multi-writer). Mock assets remain for development but are not the primary path.

**Ingestion pattern** (for each file-based asset):
```python
@asset(group_name="ingestion")
def ingest_erp_roster(context: AssetExecutionContext) -> pl.DataFrame:
    """Ingest SAP ERP roster CSV into ClickHouse bronze."""
    inbox = Path("data/inbox")
    files = sorted(inbox.glob("erp_roster_*.csv"))
    if not files:
        context.log.info("No ERP roster files in inbox")
        return pl.DataFrame()
    
    df = pl.read_csv(files[-1])  # Process most recent file
    # Validate required columns
    # Write to bronze.erp_roster (full refresh — bronze is raw)
    from dagster_clickhouse import ClickhouseResource
    with clickhouse.get_connection() as client:
        client.execute("CREATE DATABASE IF NOT EXISTS chronos")
        client.execute("CREATE OR REPLACE TABLE chronos.bronze_erp_roster AS SELECT * FROM df")
    # Archive processed file
    archive = Path("data/archive")
    files[-1].rename(archive / f"{files[-1].stem}_{timestamp()}{files[-1].suffix}")
    return df
```

### 3.3 Bronze Table Schemas

**bronze.erp_roster** (from SAP/ERP):
| Column | Type | Description |
|--------|------|-------------|
| employee_id | VARCHAR | Natural key |
| employee_name | VARCHAR | Full name |
| employee_type | VARCHAR | FULL-TIME, CONTRACTOR, INTERN |
| position_id | VARCHAR | Foreign key to position |
| position_title | VARCHAR | Current title |
| department_id | VARCHAR | Foreign key to department |
| department_name | VARCHAR | Department name |
| cost_center | VARCHAR | Financial code |
| hire_date | DATE | Employment start |
| termination_date | DATE | Employment end (NULL if active) |
| source_system | VARCHAR | Always 'ERP' |

**bronze.contractor_tracking** (from SharePoint):
| Column | Type | Description |
|--------|------|-------------|
| employee_id | VARCHAR | Contractor ID |
| employee_name | VARCHAR | Full name |
| position_id | VARCHAR | Assigned position |
| start_date | DATE | Contract start |
| end_date | DATE | Contract end |
| employee_type | VARCHAR | Always 'CONTRACTOR' |

**bronze.hr_allocations** (from SharePoint — separate from contractor tracking):
| Column | Type | Description |
|--------|------|-------------|
| employee_id | VARCHAR | Employee ID |
| employee_name | VARCHAR | Full name (messy casing) |
| position_id | VARCHAR | Assigned position |
| position_title | VARCHAR | Position title (messy casing) |
| department_id | VARCHAR | Department code |
| allocation_factor | DECIMAL | 0.0-1.0 (1.0 = full allocation) |
| assignment_start | DATE | Allocation start |
| assignment_end | DATE | Allocation end (NULL = ongoing) |

**bronze.excel_cross_reference** (from Excel):
| Column | Type | Description |
|--------|------|-------------|
| position_id | VARCHAR | Natural key |
| position_title | VARCHAR | Title override |
| department_id | VARCHAR | Department override |
| cost_center | VARCHAR | Cost center override |
| notes | VARCHAR | Free-text notes |

---

## 4. Transformation Layer (Silver)

### 4.1 Silver Transforms (Dagster Assets)

**File**: `src/chronos_seat/defs/transformation/adhoc/assets.py`

Python-based transforms that clean and standardize bronze data into silver:

- `silver_erp_roster` — Standardize column names, types, casing from ERP roster (reads `bronze.erp_roster`, writes `silver.erp_roster`)
- `silver_hr_allocations` — Standardize HR allocation data (reads `bronze.hr_allocations`, writes `silver.hr_allocations`)
- `silver_contractor_tracking` — Standardize contractor tracking data (reads `bronze.contractor_tracking`, writes `silver.contractor_tracking`)

**Pattern**: Read from `bronze.*` → clean with Polars → write to `silver.*`

### 4.2 dbt Staging Models

**Location**: `dbt_project/models/staging/`

Light validation views that read from **bronze** tables (not silver — staging models read raw data):

- `stg_erp_roster.sql` — Reads `bronze.erp_roster`, adds `_loaded_at`
- `stg_contractor_tracking.sql` — Reads `bronze.contractor_tracking`
- `stg_hr_allocations.sql` — Reads `bronze.hr_allocations`
- `stg_change_requests.sql` — Reads from change request files (via `{{ source() }}`)

**Note**: Staging models read from `bronze.*` tables directly. Silver transforms are separate Python assets (Dagster) that read bronze and write to silver. This separation keeps dbt models simple and puts complex cleaning logic in Python assets where it's easier to test.

---

## 5. Gold Layer (Dimensional Models)

### 5.1 Seeds (Reference Data)

**Location**: `dbt_project/seeds/`

- `dim_change_type.csv` — Change type reference (NEW_HIRE, EXIT, etc.)
- `dim_change_reason.csv` — Change reason reference (HIRING, REPLACEMENT, etc.)
- `dim_department.csv` — Department reference (SCD Type 1)

### 5.2 Mart Models

**Location**: `dbt_project/models/marts/`

**Macro**: `generate_sk(natural_key, effective_date)` — Generates a surrogate key using MD5 hash of natural key + effective date. Defined in `dbt_project/macros/generate_sk.sql`.

- `dim_date.sql` — 15-year date spine (2020-2034), `schema='gold'`
- `dim_position.sql` — SCD Type 2 position master, `schema='gold'`
- `dim_employee.sql` — SCD Type 2 employee master, `schema='gold'`
- `fact_position_occupancy_event.sql` — Append-only event log, `schema='gold'`
- `bridge_position_occupancy.sql` — Many-to-many with overlap tracking, `schema='gold'`

### 5.3 Intermediate Models

**Location**: `dbt_project/models/intermediate/`

- `int_change_request_events.sql` — Joins change requests with dimension tables to resolve SKs

### 5.4 Schema Tests

**File**: `dbt_project/models/marts/schema.yml`

Standard dbt tests: unique, not_null, expression_is_true, unique_combination_of_columns.

---

## 6. Change Request System

### 6.1 File Format

CSV files dropped into `data/change_requests/inbox/` with columns:
`request_id, request_date, requested_by, approved_by, effective_date, change_type, change_reason, position_id, employee_id, employee_name, employee_type, position_title, department_id, cost_center, allocation_factor, notes`

### 6.2 Sensor

**File**: `src/chronos_seat/defs/ingestion/rawgen/change_request_sensor.py`

Watches `data/change_requests/inbox/`, validates files, moves to `approved/` or `rejected/`.

### 6.3 Processing

Approved change requests flow through:
1. `stg_change_requests.sql` — Staging model reads from change request files
2. `int_change_request_events.sql` — Intermediate model joins with dimension tables to resolve SKs
3. Dagster asset `apply_change_requests` — Updates gold-layer fact table (SCD Type 2 pattern)

---

## 7. Entity Management System

### 7.1 File Format

CSV files dropped into `data/entity_requests/inbox/` with columns:
`request_id, request_date, requested_by, approved_by, effective_date, entity_type, operation, entity_id, field_name, old_value, new_value, notes`

### 7.2 Operations

- **CREATE**: Generate SK, insert new row
- **UPDATE**: Close current row, insert new row with updated fields
- **DEACTIVATE**: Set effective_end_date, is_current = FALSE
- **REACTIVATE**: Insert new row with new SK

### 7.3 Sensor + Assets

- `entity_request_sensor.py` — Watches inbox, validates, routes
- `entity_request_assets.py` — Full CRUD processing for POSITION, EMPLOYEE, DEPARTMENT

---

## 8. Dagster Definitions

**File**: `src/chronos_seat/definitions.py`

```python
from dagster import Definitions, load_assets_from_modules
from chronos_seat.defs.ingestion.rawgen import assets as ingestion_assets
from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import dbt_models
from chronos_seat.defs.transformation.dbt.resources import dbt_resource
from chronos_seat.defs.transformation.adhoc import assets as adhoc_assets
from chronos_seat.defs.ingestion.rawgen.file_sensor import file_watcher_sensor
from chronos_seat.defs.ingestion.rawgen.change_request_sensor import change_request_sensor
from chronos_seat.defs.ingestion.rawgen.entity_request_sensor import entity_request_sensor
from chronos_seat.defs.ingestion.rawgen import entity_request_assets

all_assets = [
    *load_assets_from_modules([ingestion_assets]),
    dbt_models,
    *load_assets_from_modules([adhoc_assets]),
    *load_assets_from_modules([entity_request_assets]),
]

all_sensors = [
    file_watcher_sensor,       # Watches data/inbox/ for ERP/SharePoint/Excel files
    change_request_sensor,     # Watches change_requests/inbox/
    entity_request_sensor,     # Watches entity_requests/inbox/
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

---

## 9. Rill Dashboards

**Location**: `rill_dashboard/`

- `rill.yaml` — Project config
- `sources/gold_sources.yaml` — ClickHouse source reading from gold schema
- `dashboards/position_tracker.yaml` — Main dashboard

---

## 10. Web Portal

**Location**: `portal/` (Next.js)

- Entity browser with SCD history
- Change request submission form
- Dagster and Rill iframe embeds

---

## 11. Docker Deployment

Four containers: `dagster-webserver`, `dagster-daemon`, `clickhouse`, `rill`, `portal`.

All share a named volume `chronos-data` for file ingestion inbox/archive. ClickHouse runs as a dedicated container.

---

## 15. Key Differences from Previous Architecture

| Aspect | Previous (DuckLake) | Current (ClickHouse) |
|--------|--------------------|--------------------|
| Storage | Single .ducklake file (file-level lock) | Client-server OLAP (concurrent access) |
| Concurrent users | Single writer OR reader | Multiple writers AND readers |
| Rill + Change Requests | Cannot run simultaneously | Both work at the same time |
| dbt adapter | dbt-duckdb | dbt-clickhouse |
| Dagster resource | DuckDBResource | ClickhouseResource |
| Migration reason | — | DuckLake file lock blocked multi-user access |

---

## 12. Testing

### 12.1 Makefile Targets

- `make setup` — Install deps, create directories
- `make pipeline` — Run dbt seed + build
- `make test` — Run pytest
- `make lint` — Run ruff

### 12.2 Test Files

- `tests/test_ingestion.py` — Ingestion asset tests (file reading, bronze table writes)
- `tests/test_dbt_transforms.py` — dbt model tests (gold table existence, row counts)
- `tests/test_change_requests.py` — Change request workflow tests

---

## 13. Scaling Path

### Phase 1: Local (Current)
- ClickHouse (Docker) + Local Rill + File-based ingestion
- Multi-user on LAN, zero cost

### Phase 2: Multi-Team
- ClickHouse (Docker or cloud) + Rill Cloud
- Migration: update `profiles.yml` host/port, set credentials

### Phase 3: Enterprise
- ClickHouse cluster + Dagster+ + Rill Cloud
- Migration: update dbt `profiles.yml` target, update Rill sources

---

## 14. Key Differences from v2

| Aspect | v2 | v3 |
|--------|----|----|
| Ingestion | Mock data generators wrote directly to DuckLake (file-based, single-writer) | File-based ingestion from `data/inbox/` into ClickHouse (client-server, concurrent) |
| Backend | DuckLake (file-level lock, no concurrent access) | ClickHouse (client-server OLAP, concurrent read/write) |
| Multi-user | Not supported — Rill blocks writers | Fully supported — dashboards and change requests run simultaneously |
| Production data | Not supported | SAP CSV, SharePoint CSV, Excel files |
| Sensor | None for ingestion | `file_watcher_sensor` routes by filename pattern |
| Mock assets | Primary ingestion path | Development-only fallback |
| Archive | None | Processed files moved to `data/archive/` |
| File validation | None | Required columns, readability, emptiness checks |
| Bronze tables | Written by mock generators | Written by file ingestion assets |
