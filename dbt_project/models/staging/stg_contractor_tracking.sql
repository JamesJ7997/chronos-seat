{{ config(materialized='view', schema='silver') }}

with source as (
    select *
    from bronze.contractor_tracking
),
cleaned as (
    select 
        upper(trim(contractor_id)) as employee_id,
        trim({{ initcap('contractor_name') }}) as employee_name,
        upper(trim(position_id)) as position_id,
        start_date as assignment_start,
        end_date as assignment_end,
        rate_type as employee_type,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned