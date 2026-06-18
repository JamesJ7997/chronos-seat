"""Custom Python transformations — Bronze to Silver cleaning."""

from datetime import date
import duckdb
import polars as pl
from dagster import AssetExecutionContext, AssetIn, asset


@asset(
    group_name="transformation",
    ins={"mock_erp_roster": AssetIn(key="mock_erp_roster")},
    description="Clean and standardize ERP roster to Silver",
)
def silver_erp_roster(
    context: AssetExecutionContext, mock_erp_roster: pl.DataFrame
) -> pl.DataFrame:
    """Standardize column names, types, and casing from ERP roster."""
    df = mock_erp_roster.with_columns(
        pl.col("hire_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("termination_date")
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("termination_date"),
        pl.col("employee_type").str.to_uppercase(),
        pl.col("employee_name").str.to_titlecase(),
    )

    conn = duckdb.connect("ducklake:./data/chronos.ducklake")
    conn.execute("Create table if not exists silver.erp_roster as select * from df")
    context.log.info("Wrote Silver ERP roster to DuckLake")
    return df


@asset(
    group_name="transformation",
    ins={"mock_hr_allocations": AssetIn(key="mock_hr_allocations")},
    description="Clean and standardize HR allocations to Silver",
)
def silver_hr_allocations(
    context: AssetExecutionContext, mock_hr_allocations: pl.DataFrame
) -> pl.DataFrame:
    """Fix messy casing and standardize HR allocation data."""
    df = mock_hr_allocations.rename(
        {
            "emp_id": "employee_id",
            "EmpName": "employee_name",
            "pos_id": "position_id",
            "PosTitle": "position_title",
            "dept_code": "department_id",
            "alloc_factor": "allocation_factor",
            "start_dt": "assignment_start",
            "end_dt": "assignment_end",
        }
    )

    df = df.with_columns(
        pl.col("employee_id").str.to_uppercase(),
        pl.col("employee_name").str.to_titlecase(),
        pl.col("position_id").str.to_uppercase(),
        pl.col("position_title").str.to_titlecase(),
        pl.col("department_id").str.to_uppercase(),
        pl.col("assignment_start").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("assignment_end")
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("assignment_end"),
    )

    conn = duckdb.connect("ducklake:./data/chronos.ducklake")
    conn.execute("Create table if not exists silver.hr_allocations as select * from df")
    context.log.info("Wrote Silver HR allocations to DuckLake")
    return df
