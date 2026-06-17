"""dbt resource — DbtCliResource pointing to the DbtProject directory."""

from dagster_dbt import DbtCliResource

from chronos_seat.defs.transformation.dbt.project import dbt_project

# DbtCliResource runs dbt commands (build, test, etc.) from Dagster
# The project_dir and profiles_dir point to the dbt_project/ directory
dbt_resource = DbtCliResource(
    project_dir=str(dbt_project.project_dir),  # Reuse the DbtProject path
    profiles_dir=str(dbt_project.project_dir),  # profiles.yml lives in dbt_project/
)
