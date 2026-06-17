"""Dagster-dbt integration — wraps dbt models as Dagster assets using DbtProject."""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from chronos_seat.defs.transformation.dbt.project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models as Dagster assets."""
    yield from dbt.cli(["build"], context=context).stream()
