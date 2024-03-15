{{ config(
    materialized = "incremental",
    schema = "intermediate",
    partition_by ={ "field": "bq_inserted_at",
    "data_type": "timestamp",
    "granularity": "day" },
    unique_key = "id"
) }}

WITH cte AS (

    SELECT
        id,
        profile_id,
        contact_phone,
        flow_label,
        `type` AS message_type,
        REGEXP_SUBSTR(
            -- activity status; [Activity_Submission, Activity_Sent, Activity_Access]
            flow_label,
            'Activity\\w+',
            1,
            1
        ) AS activity_status,
        REGEXP_SUBSTR(
            -- course ; eg CR00000001
            flow_label,
            'CR\\d+$',
            1,
            1
        ) AS course_id,
        REGEXP_SUBSTR(
            -- enrolled batch (there are two batche ids embedded in flow label): first occurrence ; eg BT00000002
            flow_label,
            'BT\\d+',
            1,
            1
        ) AS enrolled_batch_id,
        REGEXP_SUBSTR(
            -- current batch: second occurrence ; eg BT00000001
            flow_label,
            'BT\\d+',
            1,
            2
        ) AS batch_id,
        REGEXP_SUBSTR(
            REGEXP_SUBSTR(
                -- this regex has information about activity no and unit no
                flow_label,
                'TLM\\d{2,} - [UB]\\d{1,} - Activity \\d{1,}',
                1,
                1
            ),
            '[UB]\\d{1,}',
            -- unit ; eg U1 or B1
            1,
            1
        ) AS unit,
        REGEXP_SUBSTR(
            REGEXP_SUBSTR(
                -- this regex has information about activity no and unit no
                flow_label,
                'TLM\\d{2,} - [UB]\\d{1,} - Activity \\d{1,}',
                1,
                1
            ),
            'Activity \\d{1,}$',
            -- activity ; eg Activity 2
            1,
            1
        ) AS activity,
        inserted_at,
        updated_at,
        bq_inserted_at,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                bq_inserted_at DESC
        ) AS row_no
    FROM
        {{ source(
            "glific",
            "messages"
        ) }}
    WHERE
        {# inserted_at >= '2023-07-15T00:00:00.000000' -- when the new cycle start for 2023 #}
        {# inserted_at >= '2023-09-28T00:00:00.000000' -- this because we did structural change in crm and had to change the flow embeddings to account for it. The changes were done on this date #}
        {# inserted_at >= '2023-10-01T00:00:00.000000' -- this is to validate dashboard we need to have the same data as in the old dashboards #}
        inserted_at >= '2023-09-28T00:00:00.000000' -- this is to validate dashboard we need to have the same data as in the old dashboards

{% if is_incremental() %}
AND bq_inserted_at > (
    SELECT
        MAX(bq_inserted_at)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    *,
    -- Categorizing activities into "student", "pretest" and "engagement" (TAPs logic)
    -- "student" : activities in unit starting with 'U'
    -- "pretest" : activities in unit starting with 'B' and having activity no 5
    -- "engagement" : activities in unit starting with 'B' and having activity no != 5
    CASE
        WHEN unit LIKE 'U%' THEN "student"
        WHEN unit LIKE 'B%'
        AND activity = 'Activity 5' THEN "pretest"
        WHEN unit LIKE 'B%'
        AND activity != 'Activity 5' THEN "engagement"
        ELSE ''
    END AS activity_type,
    REGEXP_SUBSTR(
        activity,
        '[0-9]+',
        1,
        1
    ) AS activity_no,
    REGEXP_SUBSTR(
        unit,
        '[0-9]+',
        1,
        1
    ) AS unit_no,
FROM
    cte
WHERE
    row_no = 1
