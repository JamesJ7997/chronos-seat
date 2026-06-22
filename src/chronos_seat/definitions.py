from dagster import Definitions

from chronos_seat.defs.ingestion.rawgen.resources import clickhouse_resource
from chronos_seat.defs.transformation.dbt.assets import dbt_models
from chronos_seat.defs.transformation.dbt.resources import dbt_resource

all_assets = [
    dbt_models,  # dbt models as Dagster assets (bronze -> silver -> gold)
]

defs = Definitions(
    assets=all_assets,
    resources={
        # ClickhouseResource from dagster-clickhouse (managed connections)
        "clickhouse": clickhouse_resource,
        # DbtCliResource - runs dbt commands from Dagster
        "dbt": dbt_resource,
    },
)
