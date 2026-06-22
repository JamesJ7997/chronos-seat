"""Shared resources for ingestion."""

from dagster_clickhouse import ClickhouseResource

# ClickhouseResource manages ClickHouse connections via clickhouse-driver.
# All databases (chronos_bronze, chronos_silver, chronos_gold) live inside the ClickHouse server.
# Host/port match the clickhouse service in docker-compose.yml
clickhouse_resource = ClickhouseResource(
    host="localhost",
    port=9000,
    user="default",
    password="",
    database="chronos",
)
