{{ config(
    materialized = "table",
) }}

SELECT
    SAFE_CAST(inserted_at AS DATE) AS message_date,
    DATE_TRUNC(
        SAFE_CAST(inserted_at AS DATE),
        WEEK(MONDAY)
    ) AS message_week,
    DATE_TRUNC(
        SAFE_CAST(inserted_at AS DATE),
        MONTH
    ) AS message_month,
    activity_status,
    activity_type,
    course_id,
    COALESCE(
        course_id,
        'Unparsed course'
    ) AS course_display_name,
    unit,
    activity,
    COUNT(*) AS parsed_message_rows,
    COUNT(DISTINCT id) AS messages,
    COUNTIF(course_id IS NOT NULL) AS messages_with_course,
    COUNTIF(unit IS NOT NULL) AS messages_with_unit,
    COUNTIF(activity IS NOT NULL) AS messages_with_activity
FROM
    {{ ref('glific_messages_deserialize') }}
WHERE
    SAFE_CAST(inserted_at AS DATE) >= DATE('2026-01-01')
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9
