"""Shared DbtProject instance — used by both assets and resources."""

from pathlib import Path

from dagster_dbt import DbtProject

# DbtProject auto-generates the manifest and points to the dbt project directory
dbt_project = DbtProject(
    project_dir=Path(__file__).resolve().parent.parent.parent.parent.parent.parent / "dbt_project",
    prepare_project_cli_args=["--quiet"],
)
dbt_project.prepare_if_dev()  # Generates manifest.json in dev
