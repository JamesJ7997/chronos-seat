{{ config(materialized='view', schema='silver') }}

with source as (
    select * from bronze.hr_allocations
),
cleaned as (
    select
        upper(trim(emp_id)) as employee_id,
        trim({{ initcap('EmpName') }}) as employee_name,
        upper(trim(pos_id)) as position_id,
        trim({{ initcap('PosTitle') }}) as position_title,
        upper(trim(dept_code)) as department_id,
        alloc_factor as allocation_factor,
        start_dt as assignment_start,
        end_dt as assignment_end,
        current_timestamp as _loaded_at
    from source
)
select * from cleaned
