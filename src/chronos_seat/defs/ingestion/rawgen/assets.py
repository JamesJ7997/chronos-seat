"""Mock data generator — creates realistic HR data for Bronze layer."""

from datetime import datetime
import duckdb
import polars as pl
from dagster import AssetExecutionContext, asset
from pathlib import Path


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


@asset(group_name="ingestion", description="Generate mock ERP roster CSV")
def mock_erp_roster(context: AssetExecutionContext) -> pl.DataFrame:
    """Raw ERP dump — clean, consistent format."""
    data = {
        "employee_id": [f"EMP-{i:03d}" for i in range(1, 9)],
        "employee_name": [
            "Alice Johnson",
            "Bob Smith",
            "Carol Williams",
            "David Brown",
            "Eve Davis",
            "Frank Miller",
            "Grace Lee",
            "Henry Wilson",
        ],
        "employee_type": [
            "FULL-TIME",
            "FULL-TIME",
            "CONTRACTOR",
            "FULL-TIME",
            "FULL-TIME",
            "FULL-TIME",
            "CONTRACTOR",
            "FULL-TIME",
        ],
        "position_id": [
            "POS-1001",
            "POS-1002",
            "POS-1003",
            "POS-1004",
            "POS-1005",
            "POS-1006",
            "POS-1002",
            "POS-1007",
        ],
        "position_title": [
            "Sr. Data Engineer",
            "Data Analyst",
            "ML Engineer",
            "Analytics Engineer",
            "Data Engineer",
            "Staff Engineer",
            "Data Analyst",
            "VP Engineering",
        ],
        "department_id": [
            "DEPT-ENG",
            "DEPT-ENG",
            "DEPT-AIML",
            "DEPT-DATA",
            "DEPT-ENG",
            "DEPT-ENG",
            "DEPT-ENG",
            "DEPT-ENG",
        ],
        "department_name": [
            "Engineering",
            "Engineering",
            "AI/ML",
            "Data",
            "Engineering",
            "Engineering",
            "Engineering",
            "Engineering",
        ],
        "cost_center": [
            "CC-5100",
            "CC-5100",
            "CC-5200",
            "CC-5300",
            "CC-5100",
            "CC-5100",
            "CC-5100",
            "CC-5100",
        ],
        "hire_date": [
            "2021-03-15",
            "2022-07-01",
            "2024-01-10",
            "2023-06-20",
            "2020-11-08",
            "2019-04-22",
            "2025-06-01",
            "2018-01-15",
        ],
        "termination_date": ["", "", "", "", "", "", "", ""],
        "source_system": ["ERP"] * 8,
    }

    df = pl.DataFrame(data)
    conn = duckdb.connect("ducklake:./data/chronos.ducklake")
    conn.execute("Create table if not exists bronze.erp_roster as select * from df")
    context.log.info("Wrote ERP roster to bronze schema")
    return df


@asset(group_name="ingestion", description="Generate mock HR allocations Parquet")
def mock_hr_allocations(context: AssetExecutionContext) -> pl.DataFrame:
    """HR allocation list — messy casing, variable strings."""
    data = {
        "emp_id": ["EMP-001", "EMP-002", "EMP-007", "EMP-003"],
        "EmpName": ["alice johnson", "bob smith", "grace lee", "carol williams"],
        "pos_id": ["POS-1001", "POS-1002", "POS-1002", "POS-1003"],
        "PosTitle": ["sr data engineer", "data analyst", "data analyst", "ml engineer"],
        "dept_code": ["DEPT-ENG", "DEPT-ENG", "DEPT-ENG", "DEPT-AIML"],
        "alloc_factor": [1.0, 0.5, 0.5, 1.0],
        "start_dt": ["2021-03-15", "2022-07-01", "2025-06-01", "2024-01-10"],
        "end_dt": ["", "", "2025-08-31", "2025-12-31"],
    }

    df = pl.DataFrame(data)
    conn = duckdb.connect("ducklake:./data/chronos.ducklake")
    conn.execute("Create table if not exists bronze.hr_allocations as select * from df")
    context.log.info("Wrote HR allocations to bronze schema")
    return df


@asset(group_name="ingestion", description="Generate mock contractor tracking Excel")
def mock_contractor_tracking(context: AssetExecutionContext) -> pl.DataFrame:
    """Contractor tracking log — Excel with overlapping dates."""
    data = {
        "contractor_id": ["EMP-003", "EMP-007"],
        "contractor_name": ["Carol Williams", "Grace Lee"],
        "position_id": ["POS-1003", "POS-1002"],
        "start_date": ["2024-01-10", "2025-06-01"],
        "end_date": ["2025-12-31", "2025-08-31"],
        "rate_type": ["hourly", "hourly"],
    }

    df = pl.DataFrame(data)
    conn = duckdb.connect("ducklake:./data/chronos.ducklake")
    conn.execute(
        "Create table if not exists bronze.contractor_tracking as select * from df"
    )
    context.log.info("Wrote contractor tracking to bronze schema")
    return df
