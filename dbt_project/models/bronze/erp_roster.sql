{{
    config(
        materialized='table'
    )
}}
WITH
employee_count AS (
    SELECT
        80 + (toDayOfYear(today()) % 21) AS cnt
),
employees AS (
    SELECT
        number + 1 AS employee_num
    FROM numbers(
        (SELECT cnt FROM employee_count)
    )
),
positions AS (
    SELECT
        *,
        row_number() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
),
employee_base AS (
    SELECT
        employee_num,
        format('EMP-%04d', employee_num) AS employee_id,
        concat(
            arrayElement(
                [
                    'James','John','Sarah','Emma','Michael',
                    'David','Lisa','Mary','Robert','Jennifer',
                    'William','Linda','Patricia','Barbara',
                    'Daniel','Matthew','Andrew','Karen'
                ],
                (cityHash64(employee_num) % 18) + 1
            ),
            ' ',
            arrayElement(
                [
                    'Smith','Johnson','Brown','Davis','Wilson',
                    'Moore','Taylor','Thomas','White','Martin',
                    'Anderson','Jackson','Harris','Clark',
                    'Lewis','Walker','Hall','Young'
                ],
                (cityHash64(employee_num * 17) % 18) + 1
            )
        ) AS employee_name,
        arrayElement(
            [
                'FULL-TIME',
                'FULL-TIME',
                'FULL-TIME',
                'CONTRACTOR',
                'INTERN'
            ],
            (cityHash64(employee_num * 13) % 5) + 1
        ) AS employee_type,
        cityHash64(employee_num * 7) % 20 AS pos_idx,
        toDate('2023-01-01')
            + toIntervalDay(
                cityHash64(employee_num * 31)
                % (
                    dateDiff(
                        'day',
                        toDate('2023-01-01'),
                        today()
                    ) + 1
                )
            ) AS hire_date,
        cityHash64(employee_num * 19) % 100 < 15
            AS is_terminated
    FROM employees
)
SELECT
    e.employee_id,
    e.employee_name,
    e.employee_type,
    p.position_id,
    p.position_title,
    d.department_id,
    d.department_name,
    d.cost_center,
    e.hire_date,
    if(
        e.is_terminated,
        toString(
            least(
                e.hire_date
                    + toIntervalDay(
                        30 + (
                            cityHash64(employee_num * 23) % 335
                        )
                    ),
                today()
            )
        ),
        ''
    ) AS termination_date,
    'ERP' AS source_system
FROM employee_base e
JOIN positions p
    ON e.pos_idx = p.pos_idx
JOIN {{ ref('departments') }} d
    ON p.department_id = d.department_id