{{ config(materialized='view', schema='silver') }}

with source as (
    select * from bronze.contractor_tracking
),
cleaned as (
    select 
        upper(trim(employee_id)) as employee_id,
        initcap(trim(employee_name)) as employee_name,
        upper(trim(position_id)) as position_id,
        assignment_start,
        assignment_end,
        employee_type,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned