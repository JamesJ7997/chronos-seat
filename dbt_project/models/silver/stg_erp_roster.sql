{{ config(marterialized='view', schema='silver') }}

with source as (
    select * from bronze.erp_roster
),
cleaned as (
    select
        employee_id,
        trim({{ initcap('employee_name') }}) as employee_name,
        UPPER(TRIM(employee_type)) as employee_type,
        UPPER(TRIM(position_id)) as position_id,
        TRIM(position_title) as position_title,
        UPPER(TRIM(department_id)) as department_id,
        TRIM(department_name) as department_name,
        TRIM(cost_center) as cost_center,
        hire_date,
        termination_date,
        source_system,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned
