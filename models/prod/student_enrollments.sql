{{ config(
    materialized = "table",
) }}
-- this models merges courses, batches, schools and student details available on frappe for each enrollment

SELECT
    students.*,
    enrollments.enrollment_id,
    enrollments.enrollment_modified,
    enrollments.course_id,
    enrollments.course_name1,
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
