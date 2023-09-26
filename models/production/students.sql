{{ config(
    materialized = "table",
    schema = "prod"
) }}

SELECT
    student_id,
    phone,
    profile_id,
    school_id,
    school_city AS City,
    school_name AS School_Name,
    school_type AS School_Type,
    school_model AS School_Model,
    gender AS Gender,
    joined_on
FROM
    {{ ref("student_enrollments") }}
GROUP BY
    student_id,
    phone,
    profile_id,
    school_id,
    city,
    school_name,
    school_type,
    school_model,
    gender,
    joined_on
