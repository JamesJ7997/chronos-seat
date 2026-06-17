"""Shared resources for ingestion."""

from dagster_duckdb import DuckDBResource  # Built-in Dagster-DuckDB integration

# DuckDBResource is a ConfigurableResource that manages DuckDB connections.
# DuckLake: the .ducklake file is the catalog entry point; table data lives in Parquet files.
# All schemas (bronze, silver, gold) live inside the DuckLake catalog via the
# attach pattern in dbt_project/profiles.yml. Dagster connects directly via ducklake: URI.
duckdb_resource = DuckDBResource(
    # DuckLake catalog (ducklake: URI)
    database="ducklake:./dbt_project/data/gold/chronos.ducklake",
)
