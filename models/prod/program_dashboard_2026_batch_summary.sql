{{ config(
    materialized = "table",
) }}

WITH student_batch AS (

    SELECT
        batch_id,
        batch_title,
        batch_start_date,
        batch_end_date,
        batch_year,
        COUNT(DISTINCT enrollment_id) AS enrollments,
        COUNT(DISTINCT student_id) AS students,
        COUNT(DISTINCT school_id) AS schools,
        COUNT(DISTINCT course_id) AS courses
    FROM
        {{ ref('student_enrollments') }}
    WHERE
        batch_year = 2026
    GROUP BY
        1,
        2,
        3,
        4,
        5
),
message_batch AS (

    SELECT
        batch_id,
        COUNT(*) AS enrollment_message_rows,
        COUNT(DISTINCT message_id) AS messages,
        COUNT(DISTINCT enrollment_id) AS enrollments_with_messages,
        COUNT(DISTINCT student_id) AS students_with_messages,
        COUNT(DISTINCT IF(accessed = 1, student_id, NULL)) AS students_accessed,
        COUNT(DISTINCT IF(submitted = 1, student_id, NULL)) AS students_submitted,
        SUM(sent) AS sent_events,
        SUM(accessed) AS access_events,
        SUM(submitted) AS submission_events,
        MIN(SAFE_CAST(inserted_at AS TIMESTAMP)) AS first_message_at,
        MAX(SAFE_CAST(inserted_at AS TIMESTAMP)) AS last_message_at
    FROM
        {{ ref('enrollment_messages') }}
    WHERE
        batch_year = 2026
    GROUP BY
        1
),
teacher_batch AS (

    SELECT
        teacher_batch_id AS batch_id,
        COUNT(DISTINCT teacher_id) AS teachers_registered,
        COUNT(DISTINCT school_id) AS teacher_schools,
        COUNTIF(glific_id IS NOT NULL AND glific_id != '') AS teachers_with_glific_id
    FROM
        {{ ref('teacher_registrations') }}
    WHERE
        created_year = 2026
        AND teacher_batch_id IS NOT NULL
    GROUP BY
        1
)
SELECT
    student_batch.batch_id,
    student_batch.batch_title,
    student_batch.batch_start_date,
    student_batch.batch_end_date,
    student_batch.batch_year,
    student_batch.enrollments,
    student_batch.students,
    student_batch.schools,
    student_batch.courses,
    COALESCE(
        teacher_batch.teachers_registered,
        0
    ) AS teachers_registered,
    COALESCE(
        teacher_batch.teacher_schools,
        0
    ) AS teacher_schools,
    COALESCE(
        teacher_batch.teachers_with_glific_id,
        0
    ) AS teachers_with_glific_id,
    COALESCE(
        message_batch.enrollment_message_rows,
        0
    ) AS enrollment_message_rows,
    COALESCE(
        message_batch.messages,
        0
    ) AS messages,
    COALESCE(
        message_batch.enrollments_with_messages,
        0
    ) AS enrollments_with_messages,
    COALESCE(
        message_batch.students_with_messages,
        0
    ) AS students_with_messages,
    COALESCE(
        message_batch.students_accessed,
        0
    ) AS students_accessed,
    COALESCE(
        message_batch.students_submitted,
        0
    ) AS students_submitted,
    COALESCE(
        message_batch.sent_events,
        0
    ) AS sent_events,
    COALESCE(
        message_batch.access_events,
        0
    ) AS access_events,
    COALESCE(
        message_batch.submission_events,
        0
    ) AS submission_events,
    SAFE_DIVIDE(
        message_batch.access_events,
        NULLIF(
            message_batch.sent_events,
            0
        )
    ) AS access_rate,
    SAFE_DIVIDE(
        message_batch.submission_events,
        NULLIF(
            message_batch.access_events,
            0
        )
    ) AS submission_after_access_rate,
    SAFE_DIVIDE(
        message_batch.students_accessed,
        NULLIF(
            student_batch.students,
            0
        )
    ) AS student_access_rate,
    SAFE_DIVIDE(
        message_batch.students_submitted,
        NULLIF(
            student_batch.students,
            0
        )
    ) AS student_submission_rate,
    message_batch.first_message_at,
    message_batch.last_message_at
FROM
    student_batch
    LEFT JOIN message_batch
    ON student_batch.batch_id = message_batch.batch_id
    LEFT JOIN teacher_batch
    ON student_batch.batch_id = teacher_batch.batch_id
