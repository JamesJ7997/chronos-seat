"""Root Dagster definitions — merges all assets, resources, and sensors."""

from dagster import Definitions, load_assets_from_modules

from chronos_seat.defs.ingestion.rawgen import assets as ingestion_assets
from chronos_seat.defs.ingestion.rawgen.resources import duckdb_resource
from chronos_seat.defs.transformation.adhoc import assets as adhoc_assets
from chronos_seat.defs.transformation.dbt.assets import (
    dbt_models,
)  # @dbt_assets from DbtProject
from chronos_seat.defs.transformation.dbt.resources import dbt_resource

all_assets = [
    *load_assets_from_modules([ingestion_assets]),
    dbt_models,  # dbt models as Dagster assets (bronze->silver->gold)
    *load_assets_from_modules([adhoc_assets]),  # Custom Python transforms (bronze->silver cleaning)
]

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,  # DuckDBResource from dagster-duckdb
        "dbt": dbt_resource,  # DbtCliResource - runs dbt commands from Dagster
    },
)
