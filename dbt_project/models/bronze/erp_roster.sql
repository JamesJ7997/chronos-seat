{{
    config(
        materialized='table'
    )
}}
WITH
employee_count AS (
    SELECT 80 + (TODAYOFYEAR(TODAY()) % 21) AS cnt
),

employees AS (
    SELECT number + 1 AS employee_num
    FROM NUMBERS(ASSUMENOTNULL((SELECT cnt FROM employee_count)))
),

positions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
),

employee_base AS (
    SELECT
        employee_num,
        CONCAT('EMP-', LPAD(TOSTRING(employee_num), 4, '0')) AS employee_id,
        CONCAT(
            ARRAYELEMENT(
                [
                    'Alice',
                    'Bob',
                    'Carol',
                    'David',
                    'Eve',
                    'Frank',
                    'Grace',
                    'Henry',
                    'Iris',
                    'Jack',
                    'Karen',
                    'Leo',
                    'Mia',
                    'Noah',
                    'Olivia',
                    'Paul',
                    'Quinn',
                    'Rose',
                    'Sam',
                    'Tina'
                ],
                (CITYHASH64(employee_num) % 20) + 1
            ),
            ' ',
            ARRAYELEMENT(
                [
                    'Johnson',
                    'Smith',
                    'Williams',
                    'Brown',
                    'Davis',
                    'Miller',
                    'Wilson',
                    'Moore',
                    'Taylor',
                    'Anderson',
                    'Thomas',
                    'Jackson',
                    'White',
                    'Harris',
                    'Martin',
                    'Thompson',
                    'Garcia',
                    'Martinez',
                    'Robinson'
                ],
                (CITYHASH64(employee_num * 17) % 19) + 1
            )
        ) AS employee_name,
        ARRAYELEMENT(
            ['FULL-TIME', 'FULL-TIME', 'FULL-TIME', 'CONTRACTOR', 'INTERN'],
            (CITYHASH64(employee_num * 13) % 5) + 1
        ) AS employee_type,
        CITYHASH64(employee_num * 7) % 20 AS pos_idx,
        TODATE('2023-01-01')
        + TOINTERVALDAY(
            CITYHASH64(employee_num * 31)
            % (
                DATEDIFF('day', TODATE('2023-01-01'), TODAY()) + 1
            )
        ) AS hire_date,
        CITYHASH64(employee_num * 19) % 100 < 15 AS is_terminated
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
    'ERP' AS source_system,
    IF(
        e.is_terminated,
        TOSTRING(
            LEAST(
                e.hire_date
                + TOINTERVALDAY(30 + (CITYHASH64(employee_num * 23) % 335)),
                TODAY()
            )
        ),
        ''
    ) AS termination_date,
    IF(e.is_terminated, 'TERMINATED', 'ACTIVE') AS employment_status
FROM employee_base AS e
INNER JOIN positions AS p ON e.pos_idx = p.pos_idx
INNER JOIN {{ ref('departments') }} AS d ON p.department_id = d.department_id
