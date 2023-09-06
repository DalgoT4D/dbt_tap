{{ config(materialized="table", schema="dalgo_intermediate") }}

with
    cte as (
        select
            messages.contact_phone,
            messages.flow_label,
            messages.flow_name,
            messages.flow_uuid,
            messages.flow_id,
            split(flow_label, ',')[safe_ordinal(1)] as activity_status,
            split(flow_label, ',')[safe_ordinal(2)] as activity_unit_name,
            trim(
                split(split(flow_label, ',')[safe_ordinal(2)], '-')[safe_ordinal(2)]
            ) as unit,
            trim(
                split(split(flow_label, ',')[safe_ordinal(2)], '-')[safe_ordinal(3)]
            ) as activity,
        from {{ source("glific", "messages") }} as messages
    )

select
    cte.*,
    students.phone,
    students.name1,
    case when activity_status = 'Activity_Access' then 1 else 0 end as activity_access,
    case when activity_status = 'Activity_Sent' then 1 else 0 end as activity_sent,
    case
        when activity_status = 'Activity_Submission' then 1 else 0
    end as activity_submission
from cte
inner join
    {{ source("crm", "tabStudent") }} as students
    on cte.contact_phone = concat('91', students.phone)
where
    (cte.flow_label is not null)
    and (
        cte.flow_label like '%Activity_Sent%'
        or cte.flow_label like '%Activity_Access%'
        or cte.flow_label like '%Activity_Submission%'
    )
    and (cte.unit is not null)

/* types of flow label that needs to parsed
-> Activity_Access, TLM22 - B1 - Activity 1
-> Activity_Access, TLM22 - B1 - Activity 2,TLM22 - B2 - Activity 1
-> Units have 'B' as prefix so B1, B2
=> Queries to look at 
    - TAPBuddy_Activity_Performance_Status_Data (Step 1)
    - TAPBuddy_Student_Weekly_Performance (Step 2)
    - TAPBuddy_Student_Weekly_Performance_Details2 (Step 3)
    - Submission_link_extraction_for_any_Activity_TLM22 (Aggregate of all steps above)
*/
