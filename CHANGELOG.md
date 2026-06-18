# ChronosSeat CHANGELOG

All notable changes to the ChronosSeat project are documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Removed (2026-06-18)

#### Dead Code Cleanup — Removed Stale data/{raw,silver,gold} References
- **Removed `data/{raw,silver,gold}/` mkdir from quickstart**: §6.3, §12.1 Makefile, and §17.8 no longer reference these folders. DuckLake owns all Medallion layers internally.
- **Removed dead code from `rawgen/assets.py`**: Removed `RAW_PATH`, `_ensure_dir()`, all `output_path` lines, stale "Instead of" comments, and unused `date` import. Assets already wrote to DuckLake — the flat-file code was dead.
- **Removed dead code from `adhoc/assets.py`**: Removed `SILVER_PATH`, `SILVER_PATH.mkdir()` calls, all `output_path` lines, stale comments, and duplicate `return df` lines.
- **Updated quickstart callout**: "keeps catalog alongside data/{raw,silver,gold}/ directories" → "All Medallion schemas (bronze, silver, gold) live inside this single catalog."

#### Files Modified
- `Developer-Quickstart-ChronosSeat.md` — §6.3, §6.3 callout, §6.4, §12.1, §17.8
- `src/chronos_seat/defs/ingestion/rawgen/assets.py` — removed dead code
- `src/chronos_seat/defs/transformation/adhoc/assets.py` — removed dead code
- `second-brain/projects/chronos-seat/chronos-seat-developer-quickstart.md` — mirrored all doc changes

### Fixed (2026-06-18)

#### dim_date Folder Path Corrected
- **§6.4** — `dbt_project/models/gold/dim_date.sql` → `dbt_project/models/marts/dim_date.sql`. The `gold` in the description refers to the DuckLake schema (`schema='gold'`), not the folder. The folder is `marts/` per the scaffolding in §2. Actual file on disk was already in `marts/`.
- Fixed in both `Developer-Quickstart-ChronosSeat.md` and vault copy.

### Changed (2026-06-17)

#### DuckLake Architecture Overhaul
- **DuckLake catalog moved to project root**: `dbt_project/data/chronos.ducklake` → `data/chronos.ducklake`. The catalog now lives alongside `data/` at the project root, not inside `dbt_project/`.
- **DuckLake is now the single catalog for all layers**: Raw (bronver), Silver, and Gold tables all live inside DuckLake. No more flat file staging in `data/raw/` and `data/silver/`.
- **Removed `data/{raw,silver,gold}/` folders**: These are no longer needed. DuckLake's `.ducklake.files/` directory stores all table data.
- **Dagster raw assets write to DuckLake bronze**: `mock_erp_roster`, `mock_hr_allocations`, `mock_contractor_tracking` now write to `bronze.*` tables in DuckLake instead of CSV/Parquet/Excel files.
- **Dagster silver transforms write to DuckLake silver**: `silver_erp_roster`, `silver_hr_allocations`, `silver_contractor_tracking` now write to `silver.*` tables in DuckLake instead of Parquet files.
- **dbt staging models read from DuckLake bronze**: Replaced `{{ source('bronze', ...) }}` references with direct `bronze.*` table references. Removed `sources.yml` for bronze layer.
- **Added `stg_contractor_tracking.sql`**: Staging model for contractor tracking data.
- **dim_date in gold schema with marts/ folder**: `dbt_project/models/marts/dim_date.sql` with `schema='gold'`. Folder is `marts/`, schema is `gold`.
- **profiles.yml path fix**: dbt runs from `dbt_project/` so attach path uses `../data/chronos.ducklake` (relative to dbt_project/). Added `override_data_path: true` to handle existing catalog created with different path string.
- **DuckDBResource path fix**: Changed from `ducklake:./data/gold/chronos.ducklake` → `ducklake: ./data/chronos.ducklake` (removed erroneous `gold/` subdirectory).
- **All DuckLake URIs standardized**: Using `ducklake: ./data/chronos.ducklake` (with space after colon) consistently across all files.
- **.gitignore simplified**: Removed `data/raw/*`, `data/silver/*`, `data/gold/*`, `*.parquet` patterns. Now only ignores `data/chronos.ducklake` and `data/chronos.ducklake.files/`.

#### Files Modified
- `dbt_project/profiles.yml` — attach path, data_path, override_data_path
- `src/chronos_seat/defs/ingestion/rawgen/resources.py` — DuckDBResource database path
- `src/chronos_seat/defs/ingestion/rawgen/assets.py` — all 3 raw assets write to DuckLake
- `src/chronos_seat/defs/transformation/adhoc/assets.py` — all 3 silver transforms write to DuckLake
- `Developer-Quickstart-ChronosSeat.md` — comprehensive update to reflect new architecture

#### Files Not Yet Created
- `dbt_project/models/staging/stg_erp_roster.sql` — needs creation from quickstart section 6.6
- `dbt_project/models/staging/stg_hr_allocations.sql` — needs creation from quickstart section 6.6
- `dbt_project/models/staging/stg_contractor_tracking.sql` — needs creation from quickstart section 6.6
- `dbt_project/models/marts/dim_date.sql` — exists in marts/ folder, schema='gold' ✅

---

## Blueprint Eval Loop (2026-06-17)

### Process
- Ran 4-turn iterative eval loop on `chronos-seat-blueprint.md` (reconstructed from quickstart + README + agentmemory)
- Generator → Reviewer → Reviser → Re-reviewer → Decider
- Converged at 4 turns with overall score 9.5/10

### Issues Found and Fixed
1. dim_department incorrectly classified as SCD Type 2 → corrected to Type 1 seed
2. Security section completely missing → added SQL injection prevention patterns
3. Dimensional modeling conventions not documented → added FK→_sk rules, bridge PK, attribute ownership
4. stg_contractor_tracking missing from staging models → added
5. Change request processing flow unclear → clarified sensor → asset → dbt pipeline
6. Phase 2 status was IN PROGRESS → changed to COMPLETE
7. Docker volume mount for Rill dashboard missing → added
8. DAGSTER_HOME config dual-source (COPY + env var) not documented → added
9. dim_date SQL generation not noted → added note

### Output
- Versioned blueprint: `chronos-seat-blueprint_20260617_2230.md`
- Eval artifacts in `second-brain/projects/chronos-seat/eval/`

---

## Quickstart Eval Loop (2026-06-17)

### Process
- Evaluated `chronos-seat-developer-quickstart.md` against actual project files
- Checked: rawgen/assets.py, adhoc/assets.py, resources.py, profiles.yml, dim_date.sql

### Results
- All existing code files match quickstart ✅
- Staging SQL files not yet created ⚠️
- dim_date.sql exists in gold/ folder ✅


### Added (2026-06-18)

#### File-Based Ingestion Architecture (v3 Blueprint)
- **New blueprint written**: `chronos-seat-blueprint_20260618_0000.md` — v3.0 with file-based ingestion for production SAP/SharePoint/Excel sources
- **Production data sources defined**: SAP/ERP (CSV), SharePoint (CSV), Excel (.xlsx) — file drop is the primary ingestion path
- **New `data/inbox/` directory**: Drop zone for incoming production files with naming convention routing (`erp_roster_*.csv`, `sharepoint_tracking_*.csv`, `excel_crossref_*.xlsx`)
- **New `data/archive/` directory**: Processed files moved here with timestamp prefix
- **New `data/inbox/failed/` directory**: Files that failed validation
- **New `file_watcher_sensor`**: Watches `data/inbox/`, routes files by filename pattern, validates (readable, not empty, required columns, no duplicate PKs), yields `RunRequest` per file
- **New production ingestion assets**: `ingest_erp_roster`, `ingest_contractor_tracking`, `ingest_excel_crossref` — read files → write to `bronze.*` tables
- **Mock assets demoted to dev-only**: `mock_erp_roster`, `mock_hr_allocations`, `mock_contractor_tracking` remain for development but are not the production path
- **New `bronze.hr_allocations` table**: Separate from contractor tracking, stores HR allocation data from SharePoint
- **New `bronze.excel_cross_reference` table**: Stores cross-reference data from Excel files
- **New `silver_hr_allocations` transform**: Cleans `bronze.hr_allocations` → `silver.hr_allocations`
- **New `stg_hr_allocations.sql` staging model**: Reads from `bronze.hr_allocations`
- **New `generate_sk` macro**: MD5-based surrogate key generation, defined in `dbt_project/macros/generate_sk.sql`
- **New `int_change_request_events.sql` intermediate model**: Joins change requests with dimension tables to resolve SKs
- **File validation rules defined**: Readable format, not empty, required columns present, no duplicate PKs — failures go to `data/inbox/failed/`
- **Incremental strategy clarified**: Bronze = full refresh (files are snapshots), Silver/Gold = incremental where appropriate

#### Files Created
- `second-brain/projects/chronos-seat/chronos-seat-blueprint_20260618_0000.md` — v3 blueprint

#### Files Not Yet Created (Pending Implementation)
- `src/chronos_seat/defs/ingestion/rawgen/file_sensor.py` — File watcher sensor
- `dbt_project/macros/generate_sk.sql` — Surrogate key macro
- `dbt_project/models/staging/stg_hr_allocations.sql` — HR allocations staging model
- `tests/test_ingestion.py` — Ingestion tests

#### Files to Modify (Pending Implementation)
- `src/chronos_seat/defs/ingestion/rawgen/assets.py` — Add file-based ingestion assets
- `src/chronos_seat/defs/transformation/adhoc/assets.py` — Add `silver_hr_allocations` transform
- `src/chronos_seat/definitions.py` — Add `file_watcher_sensor` to sensors list
- `Makefile` — Add `data/inbox/`, `data/archive/` to setup target
- `.gitignore` — Add `data/inbox/*`, `data/archive/*` patterns

### Changed (2026-06-18)

#### Quickstart Updated for v3 Architecture
- **§2 Project Scaffolding**: Added `data/inbox/`, `data/inbox/failed/`, `data/archive/` to directory tree
- **§4 Mock Data Generator** → **§4 Ingestion Layer**: Renamed. Added file-based ingestion assets + mock assets. Added file routing table. Added file validation rules.
- **§5 Dagster Orchestration**: Added `file_sensor.py`. Updated `definitions.py` to include `file_watcher_sensor`.
- **§6.5 Silver Transforms**: Added `silver_hr_allocations` transform. Clarified bronze→silver flow.
- **§6.6 Staging Models**: Added `stg_hr_allocations.sql`. Clarified staging reads from bronze (not silver).
- **§6.7 Mart Models**: Added `generate_sk` macro definition.
- **§12 Testing**: Added `data/inbox/`, `data/archive/` to Makefile setup. Added `test_ingestion.py`.
- **§15 Quick Reference**: Updated end-to-end data flow to show file drop → inbox → sensor → bronze.
- **Header**: Updated to v6, date 2026-06-18.

#### Eval Loop on v3 Blueprint
- 15 issues found and fixed during eval:
  1. Staging models incorrectly referenced silver instead of bronze (fixed)
  2. Silver transform naming mismatch: `silver_contractor_tracking` vs `silver_hr_allocations` (fixed)
  3. Missing `int_change_request_events.sql` intermediate model (added)
  4. Missing `generate_sk` macro definition (added)
  5. Missing `bronze.hr_allocations` table schema (added)
  6. Missing `bronze.excel_cross_reference` table schema (added)
  7. File validation lacked detail (expanded with rules table)
  8. Incremental strategy was vague (clarified: bronze=full refresh, silver/gold=incremental)
  9. Docker container count said "three" but listed four (fixed)
  10. Missing `data/inbox/failed/` directory in project structure (added)
  11. Change request processing was underspecified (added 3-step flow)
  12. Missing `stg_change_requests.sql` in blueprint (noted)
  13. Missing `dbt_project/macros/` directory in project structure (added)
  14. `stg_excel_crossref.sql` staging model was in blueprint but not needed (removed — Excel crossref is for overrides, not staging)
  15. Sensor import path naming inconsistency (verified correct)

### Fixed (2026-06-18)

#### .gitignore Updated for DuckLake
- Removed stale `data/raw/*`, `data/silver/*`, `data/gold/*` patterns
- Added `data/chronos.ducklake`, `data/chronos.ducklake.files/` patterns
- Added `scripts/log-change.sh` (local helper, not tracked)

#### log-change.sh Script Rewritten
- Fixed NOVA-specific references → ChronosSeat
- Fixed sed escaping bug in multi-line insertion
- Rewrote insertion logic to use Python (reliable multi-line)
- Added category validation (Added/Changed/Fixed/Removed)
- Added proper field formatting for files changed and notes
- Synced to both project and vault CHANGELOGs


### Fixed (2026-06-18)

#### Quickstart §§1–5 Synced with Project Files
- **§2 Project Scaffolding**: Created missing `dbt_project/macros/` directory
- **§3.9 .dockerignore**: Removed stale `data/raw/*`, `data/silver/*`, `data/gold/*` patterns (DuckLake owns all layers)
- **§4.1 Sample Raw Data Format**: Updated descriptions from file paths (`data/raw/...`) to DuckLake table names (`bronze.erp_roster`, `bronze.hr_allocations`, `bronze.contractor_tracking`)
- **§4.2 Mock Data Assets**: Fixed import line `from datetime import date, datetime` → `from datetime import datetime` (matches actual code). Added note: mock assets write to DuckLake bronze; production uses file-based ingestion (§4.3).
- **§6.5 Silver Transforms (quickstart code block)**: Removed stale `SILVER_PATH = Path("data/silver")`, `from datetime import date`, `from pathlib import Path` imports. Updated `silver_contractor_tracking` to write to DuckLake `silver.contractor_tracking` instead of Parquet file.
- **§3.4 dbt_project.yml schema comments**: Updated `matches data/raw/ folder` → `DuckLake (raw ingestion layer)`, `matches data/silver/ folder` → `DuckLake (cleaned/transformed layer)`, `matches data/gold/ folder` → `DuckLake (dimensional models)`
- **§12 Testing**: Updated `test_raw_directory_structure` and `test_no_duplicate_employee_ids` to query DuckLake bronze tables instead of scanning `data/raw/` files
- **§15 Quick Reference data flow**: Updated `data/raw/ (CSV/Parquet/Excel)` → `bronze.* (DuckLake)`, `data/silver/ (cleaned Parquet)` → `silver.* (DuckLake)`
- **CI workflow test step**: Updated from file-write Python one-liner to calling `mock_erp_roster` asset directly

#### Files Modified
- `Developer-Quickstart-ChronosSeat.md` — §2, §3.4, §3.9, §4.1, §4.2, §6.5, §12, §15, CI workflow
- `second-brain/projects/chronos-seat/chronos-seat-developer-quickstart.md` — mirrored all changes
- `.dockerignore` — removed stale data/{raw,silver,gold} patterns
- `dbt_project/macros/` — created directory (was missing from scaffolding)
