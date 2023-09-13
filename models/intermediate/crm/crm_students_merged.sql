{{ config(materialized="table", schema="intermediate") }}

with
    schools as (
        select
            name as id, city as school_city, type as school_type, model as school_model,
        from {{ source("crm", "tabSchool") }}
    ),
    courses as (
        select name as id, name1, name2, type, title
        from {{ source("crm", "tabCourse") }}
    ),
    batches as (
        select name as id, name1, title, start_date, end_date
        from {{ source("crm", "tabBatch") }}
    ),
    enrollments as (
        select
            enrollment.name as enrollment_id,
            parenttype,
            parentfield,
            parent as student_id,
            course as course_id,
            courses.name1 as course_name1,
            courses.name2 as course_name2,
            courses.type as course_level,
            courses.title as course_title,
            batch as batch_id,
            batches.start_date as batch_start_date,
            batches.end_date as batch_end_date,
        from {{ source("crm", "tabEnrollment") }} as enrollment
        left join batches on batches.id = enrollment.batch
        left join courses on courses.id = enrollment.course
        where
            enrollment.parenttype = 'Student' and enrollment.parentfield = 'enrollment'
    )

select
    phone,
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
    enrollments.*
from {{ source("crm", "tabStudent") }} as students
left join schools on schools.id = students.school_id
left join enrollments on enrollments.student_id = students.name
