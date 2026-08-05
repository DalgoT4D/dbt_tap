{{ config(
    materialized = "table",
) }}

WITH student_metrics AS (

    SELECT
        COUNT(DISTINCT enrollment_id) AS enrollments,
        COUNT(DISTINCT student_id) AS students,
        COUNT(DISTINCT batch_id) AS batches,
        COUNT(DISTINCT school_id) AS schools,
        COUNT(DISTINCT course_id) AS courses
    FROM
        {{ ref('student_enrollments') }}
    WHERE
        batch_year = 2026
),
teacher_metrics AS (

    SELECT
        COUNT(DISTINCT teacher_id) AS teachers_registered,
        COUNT(DISTINCT school_id) AS teacher_schools,
        COUNTIF(glific_id IS NOT NULL AND glific_id != '') AS teachers_with_glific_id
    FROM
        {{ ref('teacher_registrations') }}
    WHERE
        created_year = 2026
),
message_metrics AS (

    SELECT
        COUNT(*) AS enrollment_message_rows,
        COUNT(DISTINCT message_id) AS messages,
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
)
SELECT
    'student_enrollments' AS metric_group,
    'students' AS metric_key,
    'Students enrolled' AS metric_label,
    CAST(student_metrics.students AS FLOAT64) AS metric_value,
    CAST(NULL AS FLOAT64) AS numerator,
    CAST(NULL AS FLOAT64) AS denominator,
    CAST(NULL AS FLOAT64) AS rate_value,
    CURRENT_TIMESTAMP() AS snapshot_at
FROM
    student_metrics
UNION ALL
SELECT
    'student_enrollments',
    'enrollments',
    'Course enrollments',
    CAST(enrollments AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    student_metrics
UNION ALL
SELECT
    'student_enrollments',
    'schools',
    'Schools represented',
    CAST(schools AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    student_metrics
UNION ALL
SELECT
    'teacher_registration',
    'teachers_registered',
    'Teachers registered',
    CAST(teachers_registered AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    teacher_metrics
UNION ALL
SELECT
    'teacher_registration',
    'teacher_schools',
    'Schools with registered teachers',
    CAST(teacher_schools AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    teacher_metrics
UNION ALL
SELECT
    'teacher_registration',
    'teacher_glific_sync_rate',
    'Teachers with Glific ID',
    SAFE_DIVIDE(
        teachers_with_glific_id,
        teachers_registered
    ),
    CAST(teachers_with_glific_id AS FLOAT64),
    CAST(teachers_registered AS FLOAT64),
    SAFE_DIVIDE(
        teachers_with_glific_id,
        teachers_registered
    ),
    CURRENT_TIMESTAMP()
FROM
    teacher_metrics
UNION ALL
SELECT
    'student_activity',
    'messages_sent',
    'Activity messages sent',
    CAST(sent_events AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    message_metrics
UNION ALL
SELECT
    'student_activity',
    'students_accessed_activities',
    'Students who accessed activities',
    CAST(students_accessed AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    message_metrics
UNION ALL
SELECT
    'student_activity',
    'students_submitted_activities',
    'Students who submitted activities',
    CAST(students_submitted AS FLOAT64),
    NULL,
    NULL,
    NULL,
    CURRENT_TIMESTAMP()
FROM
    message_metrics
UNION ALL
SELECT
    'student_activity',
    'student_access_rate',
    'Student access rate',
    SAFE_DIVIDE(
        students_accessed,
        students
    ),
    CAST(students_accessed AS FLOAT64),
    CAST(students AS FLOAT64),
    SAFE_DIVIDE(
        students_accessed,
        students
    ),
    CURRENT_TIMESTAMP()
FROM
    message_metrics
    CROSS JOIN student_metrics
UNION ALL
SELECT
    'student_activity',
    'student_submission_rate',
    'Student submission rate',
    SAFE_DIVIDE(
        students_submitted,
        students
    ),
    CAST(students_submitted AS FLOAT64),
    CAST(students AS FLOAT64),
    SAFE_DIVIDE(
        students_submitted,
        students
    ),
    CURRENT_TIMESTAMP()
FROM
    message_metrics
    CROSS JOIN student_metrics
