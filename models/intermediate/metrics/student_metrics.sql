{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

SELECT
    *
FROM
    {{ ref("student_enrollments") }} AS students
    INNER JOIN {{ ref("glific_messages_deserialize") }} AS messages
    ON CONCAT(
        '91',
        students.phone
    ) = messages.contact_phone
WHERE
    messages.activity_status IS NOT NULL
    AND students.course_id = messages.course_id
    AND students.batch_id = messages.batch_id
