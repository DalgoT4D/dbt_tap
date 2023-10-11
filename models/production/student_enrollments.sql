{{ config(
    materialized = "incremental",
    schema = "prod",
    unique_key = "enrollment_id"
) }}
-- this models merges courses, batches, schools and student details available on frappe for each enrollment
WITH schools AS (

    SELECT
        `name` AS id,
        city AS school_city,
        `type` AS school_type,
        model AS school_model,
        name1 AS school_name,
        modified AS school_modified
    FROM
        {{ source(
            "crm",
            "tabSchool"
        ) }}
),
courses AS (
    SELECT
        `name` AS id,
        name1,
        name2,
        modified AS course_modified
    FROM
        {{ source(
            "crm",
            "tabCourse"
        ) }}
),
batches AS (
    SELECT
        NAME AS id,
        name1,
        title,
        start_date,
        end_date,
        modified AS batch_modified
    FROM
        {{ source(
            "crm",
            "tabBatch"
        ) }}
),
enrollments AS (
    SELECT
        enrollment.name AS enrollment_id,
        parenttype,
        parentfield,
        modified,
        `parent` AS student_id,
        course AS course_id,
        courses.name1 AS course_name1,
        courses.name2 AS course_name2,
        courses.course_modified,
        batch AS batch_id,
        batches.start_date AS batch_start_date,
        batches.end_date AS batch_end_date,
        batches.title AS batch_title,
        batches.batch_modified,
    FROM
        {{ source(
            "crm",
            "tabEnrollment"
        ) }} AS enrollment
        LEFT JOIN batches
        ON batches.id = enrollment.batch
        LEFT JOIN courses
        ON courses.id = enrollment.course
    WHERE
        enrollment.parenttype = 'Student'
        AND enrollment.parentfield = 'enrollment'
)
SELECT
    phone,
    profile_id,
    alt_phone,
    grade,
    section,
    gender,
    shared_users,
    joined_on,
    school_id,
    schools.school_city,
    schools.school_type,
    schools.school_model,
    schools.school_name,
    schools.school_modified,
    enrollments.*,
    EXTRACT(
        YEAR
        FROM
            batch_start_date
    ) AS batch_year,
    GREATEST(
        enrollments.batch_modified,
        enrollments.course_modified,
        schools.school_modified,
        enrollments.modified,
        students.modified
    ) AS last_sync_time
FROM
    {{ source(
        "crm",
        "tabStudent"
    ) }} AS students
    LEFT JOIN schools
    ON schools.id = students.school_id
    LEFT JOIN enrollments
    ON enrollments.student_id = students.name

{% if is_incremental() %}
WHERE
    GREATEST(
        enrollments.batch_modified,
        enrollments.course_modified,
        schools.school_modified,
        enrollments.modified,
        students.modified
    ) > (
        SELECT
            MAX(
                last_sync_time
            )
        FROM
            {{ this }}
    )
{% endif %}
