{{ config(materialized="table", schema="dalgo_intermediate") }}

-- The date filter on inserted_at marks the start of the new (2023) registration cycle
-- activity status can be Activity_Submission, Activity_Sent and Activity_Access
select
    id,
    contact_phone,
    flow_label,
    type as message_type,
    regexp_substr(flow_label, 'Activity\\w+', 1, 1) as activity_status,
    regexp_substr(flow_label, 'CR\\d+$', 1, 1) as course_id,
    regexp_substr(flow_label, 'BT\\d+', 1, 1) as enrolled_batch_id,
    regexp_substr(flow_label, 'BT\\d+', 1, 2) as batch_id,
    inserted_at
from {{ source("glific", "messages") }}
where inserted_at >= '2023-07-15T00:00:00.000000'
