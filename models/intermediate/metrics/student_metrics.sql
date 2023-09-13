{{ config(materialized="table", schema="intermediate") }}

select *
from {{ ref("crm_students_merged") }} as students
inner join
    {{ ref("glific_messages_deserialize") }} as messages
    on concat('91', students.phone) = messages.contact_phone
where
    messages.activity_status is not null
    and students.course_id = messages.course_id
    and students.batch_id = messages.batch_id
