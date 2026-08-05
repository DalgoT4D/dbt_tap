{{ config(
    materialized = "table",
) }}

SELECT
    SAFE_CAST(inserted_at AS DATE) AS activity_date,
    DATE_TRUNC(
        SAFE_CAST(inserted_at AS DATE),
        WEEK(MONDAY)
    ) AS activity_week,
    DATE_TRUNC(
        SAFE_CAST(inserted_at AS DATE),
        MONTH
    ) AS activity_month,
    batch_id,
    batch_title,
    course_id,
    COALESCE(
        NULLIF(
            course_name1,
            ''
        ),
        NULLIF(
            course_name2,
            ''
        ),
        course_id
    ) AS course_display_name,
    course_name1,
    course_name2,
    activity_type,
    unit,
    activity,
    activity_status,
    COUNT(*) AS enrollment_message_rows,
    COUNT(DISTINCT message_id) AS messages,
    COUNT(DISTINCT enrollment_id) AS enrollments,
    COUNT(DISTINCT student_id) AS students,
    SUM(sent) AS sent_events,
    SUM(accessed) AS access_events,
    SUM(submitted) AS submission_events
FROM
    {{ ref('enrollment_messages') }}
WHERE
    batch_year = 2026
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13
