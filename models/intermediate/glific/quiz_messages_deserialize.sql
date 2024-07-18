{{ config(
    materialized = "incremental",
) }}

SELECT
    id,
    profile_id,
    contact_phone,
    flow_label,
    flow_name,
    `type` AS message_type,
    REGEXP_SUBSTR(
        -- activity status; [Activity_Submission, Activity_Sent, Activity_Access]
        flow_label,
        'Activity\\w+',
        1,
        1
    ) AS activity_status,
    REGEXP_SUBSTR(
        -- Question for the quiz; eg Question_1, Question_2 ....
        flow_label,
        'Question_\\d+',
        1,
        1
    ) AS quiz_question,
    CAST(SPLIT(flow_label, ',') [SAFE_ORDINAL(5)] AS INT) AS quiz_score,
    TRIM(SPLIT(flow_label, ',') [SAFE_ORDINAL(2)]) AS course,
    REGEXP_SUBSTR(
        -- Extract TLM...
        flow_label,
        'TLM\\d{2}',
        1,
        1
    ) AS academic_year,
    REGEXP_SUBSTR(
        -- course ; eg CR00000001
        flow_label,
        'CR\\d+$',
        1,
        1
    ) AS course_id,
    REGEXP_SUBSTR(
        -- enrolled batch (there are two batch ids embedded in flow label): first occurrence ; eg BT00000002
        flow_label,
        'BT\\d+',
        1,
        1
    ) AS enrolled_batch_id,
    COALESCE(
        REGEXP_SUBSTR(
            -- current batch: second occurrence ; eg BT00000001
            flow_label,
            'BT\\d+',
            1,
            2
        ),
        REGEXP_SUBSTR(
            -- enrolled batch (if current batch is not present take the enrolled batch)
            flow_label,
            'BT\\d+',
            1,
            1
        )
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
    CAST(
        REGEXP_SUBSTR(
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
            ),
            '\\d+',
            1,
            1
        ) AS INT
    ) AS activity_no,
    5 AS max_quiz_score,
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
    -- (
    --     inserted_at >= '2024-07-03T00:00:00.000000'
    -- ) -- this is to validate quiz logic
    -- AND
    (
        flow_name = 'Template quiz flow question - 5' -- hardcoded for now
        AND flow_label LIKE '%Question_5%' --only Question_5 (final/last question) has the quiz score
    )

{% if is_incremental() %}
AND (
    bq_inserted_at > (
        SELECT
            MAX(bq_inserted_at)
        FROM
            {{ this }}
    )
)
{% endif %}
