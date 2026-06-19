{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with allocations as (
    select
        employee_id,
        position_id,
        assignment_start::Date as assignment_start,
        coalesce(nullif(assignment_end, ''), '9999-12-31')::Date as assignment_end,
        allocation_factor
    from {{ ref('stg_hr_allocations') }}
),
final as (
    select 
        dp.position_sk,
        de.employee_sk,
        a.assignment_start,
        a.assignment_end,
        a.allocation_factor,
        case 
            when exists(
                select 1 from allocations a2
                where a2.position_id = a.position_id
                and a2.employee_id != a.employee_id
                and a2.assignment_start <= a.assignment_end
                and a2.assignment_end >= a.assignment_start
            ) then True
            else False
        end as is_overlap
    from allocations a 
    left join {{ ref('dim_position') }} dp on a.position_id = dp.position_id and dp.is_current = true 
    left join {{ ref('dim_employee') }} de on a.employee_id = de.employee_id and de.is_current = true 
)
select * from final