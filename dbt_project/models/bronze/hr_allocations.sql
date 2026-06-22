{{
    config(
        materialized='table'
    )
}}
WITH
allocation_count AS (
    SELECT 80 + (TODAYOFYEAR(TODAY()) % 21) AS cnt
),

allocations AS (
    SELECT number + 1 AS allocation_num
    FROM NUMBERS(
        ASSUMENOTNULL((SELECT cnt FROM allocation_count))
    )
),

positions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
),

allocation_base AS (
    SELECT
        allocation_num,
        CONCAT(
            'EMP-',
            LPAD(TOSTRING((CITYHASH64(allocation_num * 5) % 100) + 1), 4, '0')
        ) AS emp_id,
        IF(
            CITYHASH64(allocation_num) % 2 = 0,
            LOWER(
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
                        (CITYHASH64(allocation_num * 41) % 20) + 1
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
                        (CITYHASH64(allocation_num * 43) % 19) + 1
                    )
                )
            ),
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
                    (CITYHASH64(allocation_num * 41) % 20) + 1
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
                    (CITYHASH64(allocation_num * 43) % 19) + 1
                )
            )
        ) AS empname,
        CITYHASH64(allocation_num * 7) % 20 AS pos_idx,
        ARRAYELEMENT(
            [0.25, 0.5, 0.75, 1.0, 1.0, 1.0],
            (CITYHASH64(allocation_num * 11) % 6) + 1
        ) AS alloc_factor,
        TODATE('2023-01-01')
        + TOINTERVALDAY(
            CITYHASH64(allocation_num * 29)
            % (
                DATEDIFF(
                    'day',
                    TODATE('2023-01-01'),
                    TODAY()
                ) + 1
            )
        ) AS start_dt,
        CITYHASH64(allocation_num * 17) % 100 < 40
            AS has_end_date
    FROM allocations
)

SELECT
    a.emp_id,
    a.empname,
    p.position_id AS pos_id,
    p.department_id AS dept_code,
    a.alloc_factor,
    a.start_dt,
    MULTIIF(
        CITYHASH64(a.allocation_num * 3) % 10 < 3,
        LOWER(p.position_title),
        CITYHASH64(a.allocation_num * 3) % 10 < 6,
        INITCAP(p.position_title),
        p.position_title
    ) AS postitle,
    IF(
        a.has_end_date,
        TOSTRING(
            LEAST(
                a.start_dt
                + TOINTERVALDAY(
                    30
                    + (
                        CITYHASH64(a.allocation_num * 19)
                        % 335
                    )
                ),
                TODAY()
            )
        ),
        ''
    ) AS end_dt
FROM allocation_base AS a
INNER JOIN positions AS p
    ON a.pos_idx = p.pos_idx
