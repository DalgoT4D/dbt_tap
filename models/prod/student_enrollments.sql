{{ config(
    materialized = "table",
) }}
-- this merges enrollments with students table

SELECT
    students.*,
    enrollments.enrollment_id,
    enrollments.enrollment_modified,
    enrollments.course_id,
    enrollments.course_name1,
    enrollments.course_name2,
    enrollments.batch_id,
    enrollments.batch_start_date,
    enrollments.batch_end_date,
    enrollments.batch_title,
    EXTRACT(
        YEAR
        FROM
            batch_start_date
    ) AS batch_year
FROM
    {{ ref('enrollments') }} AS enrollments
    INNER JOIN {{ ref('students') }} AS students
    ON enrollments.student_id = students.student_id
