{{ config(
    materialized = "table",
) }}

SELECT
    enrollment.name AS enrollment_id,
    enrollment.parent AS student_id,
    enrollment.modified AS enrollment_modified,
    course.name AS course_id,
    course.name1 AS course_name1,
    course.name2 AS course_name2,
    batch.name AS batch_id,
    batch.start_date AS batch_start_date,
    batch.end_date AS batch_end_date,
    batch.title AS batch_title
FROM
    {{ source(
        "crm",
        "tabEnrollment"
    ) }} AS enrollment
    INNER JOIN {{ source(
        'crm',
        'tabBatch'
    ) }}
    batch
    ON enrollment.batch = batch.name
    INNER JOIN {{ source(
        'crm',
        'tabCourse'
    ) }}
    course
    ON enrollment.course = course.name
WHERE
    parenttype = 'Student'
    AND parentfield = 'enrollment'
