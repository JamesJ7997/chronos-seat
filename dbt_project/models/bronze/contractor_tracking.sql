{{
    config(
        materialized='table'
    )
}}
WITH contractors AS (
    SELECT
        number + 1 AS contractor_num,
        TODATE('2023-01-01')
        + TOINTERVALDAY(
            CITYHASH64((number + 1) * 31)
            % DATEDIFF('day', TODATE('2023-01-01'), TODAY())
        ) AS start_date
    FROM NUMBERS(100)
),

positions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY position_id) - 1 AS pos_idx
    FROM {{ ref('positions') }}
)

SELECT
    p.position_id,
    c.start_date,
    CONCAT('CTR-', LPAD(TOSTRING(contractor_num), 4, '0')) AS contractor_id,
    CONCAT('Contractor ', contractor_num) AS contractor_name,
    IF(
        CITYHASH64(contractor_num * 17) % 100 < 40,
        TOSTRING(
            LEAST(
                c.start_date + TOINTERVALDAY(
                    30 + CITYHASH64(contractor_num * 19) % 335
                ),
                TODAY()
            )
        ),
        ''
    ) AS end_date,
    ARRAYELEMENT(
        ['hourly', 'daily', 'fixed'],
        (CITYHASH64(contractor_num * 23) % 3) + 1
    ) AS rate_type
FROM contractors AS c
INNER JOIN positions AS p
    ON (CITYHASH64(contractor_num * 7) % 20) = p.pos_idx
