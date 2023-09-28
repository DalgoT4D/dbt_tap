{{ config(
    materialized = "table",
    schema = "prod"
) }}

WITH cte AS (

    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY phone,
            msg_profile_id,
            course_id,
            batch_id,
            unit,
            activity,
            activity_status
            ORDER BY
                inserted_at
        ) AS row_num
    FROM
        {{ ref("enrollment_messages") }}
)
SELECT
    *
FROM
    cte
WHERE
    row_num = 1
