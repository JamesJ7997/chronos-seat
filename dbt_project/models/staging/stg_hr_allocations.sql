{{ config(materialized='view', schema='silver') }}

with source as (
    select * from bronze.hr_allocations
),
cleaned as (
    select 
        upper(trim(employee_id)) as employee_id,
        initcap(trim(employee_name)) as employee_name,
        upper(trim(position_id)) as position_id,
        initcap(trim(position_title)) as position_title,
        upper(trim(department_id)) as department_id,
        allocatin_factor,
        assignment_start,
        assignment_end,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned