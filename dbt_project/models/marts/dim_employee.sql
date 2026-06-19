{{
    config(
        materialized='table',
        schema='gold',
        unique_key='employee_sk'
    )
}}

with source as (
    select distinct 
        employee_id,
        employee_name,
        employee_type,
        hire_date
    from {{ ref('stg_erp_roster') }}
),
final as (
    select
        {{ generate_sk('employee_id', 'hire_date') }} as employee_sk,
        employee_id,
        employee_name,
        employee_type,
        hire_date,
        Null as termination_date,
        True as is_current,
        hire_date::Date as effective_start_date,
        '9999-12-31'::Date as effective_end_date,
        'ERP' as source_system,
        current_timestamp as inserted_at,
        current_timestamp as _loaded_at
    from source
)
select * from final