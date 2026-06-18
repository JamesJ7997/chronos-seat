{#
    Override dbt's default generate_schema_name to avoid the `{database}_{schema}` prefix.

    By default, dbt generates schema names as `{target.schema}_{custom_schema_name}`,
    which in DuckDB/DuckLake produces `main_bronze` instead of `bronze`.
    This macro returns just the custom schema name (from +schema: config),
    so the DuckLake catalog uses clean names: bronze, silver, gold.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}