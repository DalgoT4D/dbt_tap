{{ config(
    materialized = "table",
    schema = "intermediate"
) }}
-- The date filter on inserted_at marks the start of the new (2023) registration cycle
-- activity status can be Activity_Submission, Activity_Sent and Activity_Access

SELECT
    id,
    profile_id,
    contact_phone,
    flow_label,
    `type` AS message_type,
    REGEXP_SUBSTR(
        flow_label,
        'Activity\\w+',
        1,
        1
    ) AS activity_status,
    REGEXP_SUBSTR(
        flow_label,
        'CR\\d+$',
        1,
        1
    ) AS course_id,
    REGEXP_SUBSTR(
        flow_label,
        'BT\\d+',
        1,
        1
    ) AS enrolled_batch_id,
    REGEXP_SUBSTR(
        flow_label,
        'BT\\d+',
        1,
        2
    ) AS batch_id,
    REGEXP_SUBSTR(
        REGEXP_SUBSTR(
            flow_label,
            'TLM\\d{2,} - [UB]\\d{1,} - Activity \\d{1,}',
            1,
            1
        ),
        '[UB]\\d{1,}',
        1,
        1
    ) AS unit,
    REGEXP_SUBSTR(
        REGEXP_SUBSTR(
            flow_label,
            'TLM\\d{2,} - [UB]\\d{1,} - Activity \\d{1,}',
            1,
            1
        ),
        'Activity \\d{1,}$',
        1,
        1
    ) AS activity,
    inserted_at
FROM
    {{ source(
        "glific",
        "messages"
    ) }}
WHERE
    inserted_at >= '2023-07-15T00:00:00.000000'
