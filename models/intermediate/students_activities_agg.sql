{{ config(materialized="table", schema="dalgo_intermediate") }}

with
    cte as (
        select
            name1,
            phone,
            unit,
            activity,
            sum(activity_sent) as total_sent,
            sum(activity_access) as total_accessed,
            sum(activity_submission) as total_submitted,
        from {{ ref("activities_mapped_crm") }}
        group by name1, phone, unit, activity
    )

select
    cte.name1,
    cte.phone,
    round(sum(total_accessed) / sum(total_sent) * 100, 2) as access_rate,
    round(sum(total_submitted) / sum(total_sent) * 100, 2) as submission_rate
from cte
group by name1, phone
order by submission_rate desc
