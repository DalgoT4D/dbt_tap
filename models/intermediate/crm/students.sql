{{ config(
    materialized = "table",
) }}

SELECT
    student.name AS student_id,
    student.name1 AS student_name,
    student.profile_id,
    student.phone,
    student.alt_phone,
    student.grade,
    student.section,
    student.gender,
    student.shared_users,
    student.joined_on,
    school.name AS school_id,
    school.city AS school_city,
    school.model AS school_model,
    school.name1 AS school_name,
    school.type AS school_type,
    school.modified AS school_modified
FROM
    {{ source(
        'crm',
        'tabStudent'
    ) }} AS student
    LEFT JOIN {{ source(
        'crm',
        'tabSchool'
    ) }} AS school
    ON student.school_id = school.name
