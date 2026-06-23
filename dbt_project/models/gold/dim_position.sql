{{
    config(
        materialized='table',
        schema='gold',
        unique_key='position_sk'
    )
}}

WITH source AS (
    SELECT DISTINCT
        position_id,
        position_title,
        department_id,
        cost_center
    FROM {{ ref('stg_erp_roster') }}
),

final AS (
    SELECT
        {{ generate_sk('position_id', "'" ~ var("dim_effective_start", "2025-01-01") ~ "'") }} AS position_sk,
        position_id,
        position_title,
        department_id,
        cost_center,
        NULL AS budgeted_salary_band,
        FALSE AS is_manager_position,
        NULL AS manager_sk,
        DATE '{{ var('dim_effective_start', '2025-01-01') }}' AS effective_start_date,
        DATE '9999-12-31' AS effective_end_date,
        TRUE AS is_current,
        'ERP' AS source_system,
        current_timestamp AS inserted_at,
        current_timestamp AS _loaded_at
    FROM source
)

SELECT * FROM final
