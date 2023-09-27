{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

SELECT
    enrollments.student_id,
    enrollments.phone,
    enrollments.profile_id,
    enrollments.course_id,
    enrollments.course_name1,
    enrollments.course_name2,
    enrollments.batch_id,
    enrollments.batch_start_date,
    enrollments.batch_end_date,
    enrollments.batch_title,
    messages.id AS message_id,
    messages.activity_status,
    messages.message_type,
    CASE
        WHEN messages.activity_status = 'Activity_Sent' THEN 1
        ELSE 0
    END AS `sent`,
    CASE
        WHEN messages.activity_status = 'Activity_Access' THEN 1
        ELSE 0
    END AS `accessed`,
    CASE
        WHEN messages.activity_status = 'Activity_Submission' THEN 1
        ELSE 0
    END AS `submitted`,
FROM
    {{ ref("student_enrollments") }} AS enrollments
    INNER JOIN {{ ref("glific_messages_deserialize") }} AS messages
    ON (
        CONCAT(
            '91',
            enrollments.phone
        ) = messages.contact_phone
        AND enrollments.course_id = messages.course_id
        AND enrollments.batch_id = messages.batch_id
        OR enrollments.profile_id = CAST(
            messages.profile_id AS STRING
        )
    )
WHERE
    messages.activity_status IS NOT NULL
