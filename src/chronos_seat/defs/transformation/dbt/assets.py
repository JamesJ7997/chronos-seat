"""Dagster-dbt integration - wraps dbt models as Dagster assets using DbtProject"""

from chronos_seat.defs.transformation.dbt.project import dbt_project
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models as Dagster assets"""
    yield from dbt.cli(["build"], context=context).stream()
