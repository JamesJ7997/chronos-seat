{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='event_id'
    )
}}

with source as (
    select 
        employee_id,
        position_id,
        hire_date::Date as effective_date
    from {{ ref('stg_erp_roster') }}
    where employee_type = 'FULL-TIME'
),
events as (
    select 
        md5(s.employee_id || '-' || s.position_id || '-' || cast(s.effective_date as varchar)) as event_id,
        dp.position_id,
        de.employee_sk,
        'NEW_HIRE' as change_type_sk,
        'system' as requested_by,
        Null as approved_by,
        current_timestamp as event_timestamp,
        s.effective_date,
        s.effective_date as requested_date,
        Null as change_reason_sk,
        'Initial load' as change_notes,
        Null as old_value,
        json_object('employee_id', s.employee_id, 'position_id', s.position_id) as new_value,
        1 as event_version,
        Null as superseded_by,
        md5(cast(current_timestamp as varchar)) as batch_id
    from source s
        left join {{ ref('dim_position') }} dp on s.position_id = dp.position_id and dp.is_current = True 
        left join {{ ref('dim_employee') }} de on s.employee_id = de.employee_id and de.is_current = True
)
select * from events
