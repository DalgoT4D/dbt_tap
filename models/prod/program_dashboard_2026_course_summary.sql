{{ config(
    materialized = "table",
) }}

WITH student_course AS (

    SELECT
        course_id,
        course_name1,
        course_name2,
        COUNT(DISTINCT enrollment_id) AS enrollments,
        COUNT(DISTINCT student_id) AS students,
        COUNT(DISTINCT batch_id) AS batches,
        COUNT(DISTINCT school_id) AS schools
    FROM
        {{ ref('student_enrollments') }}
    WHERE
        batch_year = 2026
    GROUP BY
        1,
        2,
        3
),
message_course AS (

    SELECT
        course_id,
        COUNT(*) AS enrollment_message_rows,
        COUNT(DISTINCT message_id) AS messages,
        COUNT(DISTINCT student_id) AS students_with_messages,
        COUNT(DISTINCT IF(accessed = 1, student_id, NULL)) AS students_accessed,
        COUNT(DISTINCT IF(submitted = 1, student_id, NULL)) AS students_submitted,
        SUM(sent) AS sent_events,
        SUM(accessed) AS access_events,
        SUM(submitted) AS submission_events
    FROM
        {{ ref('enrollment_messages') }}
    WHERE
        batch_year = 2026
    GROUP BY
        1
)
SELECT
    student_course.course_id,
    COALESCE(
        NULLIF(
            student_course.course_name1,
            ''
        ),
        NULLIF(
            student_course.course_name2,
            ''
        ),
        student_course.course_id
    ) AS course_display_name,
    student_course.course_name1,
    student_course.course_name2,
    student_course.enrollments,
    student_course.students,
    student_course.batches,
    student_course.schools,
    COALESCE(
        message_course.enrollment_message_rows,
        0
    ) AS enrollment_message_rows,
    COALESCE(
        message_course.messages,
        0
    ) AS messages,
    COALESCE(
        message_course.students_with_messages,
        0
    ) AS students_with_messages,
    COALESCE(
        message_course.students_accessed,
        0
    ) AS students_accessed,
    COALESCE(
        message_course.students_submitted,
        0
    ) AS students_submitted,
    COALESCE(
        message_course.sent_events,
        0
    ) AS sent_events,
    COALESCE(
        message_course.access_events,
        0
    ) AS access_events,
    COALESCE(
        message_course.submission_events,
        0
    ) AS submission_events,
    SAFE_DIVIDE(
        message_course.access_events,
        NULLIF(
            message_course.sent_events,
            0
        )
    ) AS access_rate,
    SAFE_DIVIDE(
        message_course.submission_events,
        NULLIF(
            message_course.access_events,
            0
        )
    ) AS submission_after_access_rate
FROM
    student_course
    LEFT JOIN message_course
    ON student_course.course_id = message_course.course_id
