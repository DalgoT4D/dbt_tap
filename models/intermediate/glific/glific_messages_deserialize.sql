{{ config(
    materialized = "incremental",
    schema = "intermediate",
    partition_by ={ "field": "bq_inserted_at",
    "data_type": "timestamp",
    "granularity": "day" },
    unique_key = "id"
) }}

WITH crm_courses AS (

    SELECT
        course_id,
        course_area,
        course_level,
        COUNT(*) AS enrollment_rows
    FROM
        (
            SELECT
                enrollment.course AS course_id,
                CASE
                    WHEN REGEXP_CONTAINS(
                        enrollment.course,
                        r'^[^-]+-[^-]+-Level [0-9A-Za-z]+$'
                    ) THEN SPLIT(
                        enrollment.course,
                        '-'
                    ) [SAFE_OFFSET(1)]
                    WHEN REGEXP_CONTAINS(
                        enrollment.course,
                        r'^Level [^-]+-[^-]+-[^-]+-'
                    ) THEN SPLIT(
                        enrollment.course,
                        '-'
                    ) [SAFE_OFFSET(2)]
                    WHEN REGEXP_CONTAINS(
                        enrollment.course,
                        r'^Level [^-]+-[^-]+-'
                    ) THEN SPLIT(
                        enrollment.course,
                        '-'
                    ) [SAFE_OFFSET(1)]
                    ELSE NULL
                END AS course_area,
                CASE
                    WHEN REGEXP_CONTAINS(
                        enrollment.course,
                        r'^[^-]+-[^-]+-Level [0-9A-Za-z]+$'
                    ) THEN SPLIT(
                        enrollment.course,
                        '-'
                    ) [SAFE_OFFSET(2)]
                    WHEN REGEXP_CONTAINS(
                        enrollment.course,
                        r'^Level [^-]+-'
                    ) THEN SPLIT(
                        enrollment.course,
                        '-'
                    ) [SAFE_OFFSET(0)]
                    ELSE NULL
                END AS course_level
            FROM
                {{ source(
                    "crm",
                    "tabEnrollment"
                ) }} AS enrollment
            WHERE
                enrollment.course IS NOT NULL
                AND enrollment.course != ''
        )
    GROUP BY
        1,
        2,
        3
),
messages AS (

    SELECT
        id,
        profile_id,
        contact_phone,
        flow_label,
        `type` AS message_type,
        REGEXP_EXTRACT(
            -- activity status; [Activity_Submission, Activity_Sent, Activity_Access]
            flow_label,
            r'Activity\w+'
        ) AS activity_status,
        REGEXP_EXTRACT(
            -- legacy course id; eg CR00000001
            flow_label,
            r'CR\d+$'
        ) AS legacy_course_id,
        REGEXP_EXTRACT_ALL(
            flow_label,
            r'BT\d+'
        ) AS batch_ids,
        REGEXP_EXTRACT(
            REGEXP_EXTRACT(
                -- legacy unit/activity labels; eg TLM25 - U2 - Activity RA7
                flow_label,
                r'TLM\d{2,}\s*-\s*[UB]\d{1,}\s*-\s*Activity\s+[A-Za-z0-9 ]+'
            ),
            r'[UB]\d{1,}'
        ) AS legacy_unit,
        REGEXP_EXTRACT(
            REGEXP_EXTRACT(
                flow_label,
                r'TLM\d{2,}\s*-\s*[UB]\d{1,}\s*-\s*Activity\s+[A-Za-z0-9 ]+'
            ),
            r'Activity\s+[A-Za-z0-9 ]+$'
        ) AS legacy_activity,
        REGEXP_EXTRACT(
            -- newer compact labels; eg TLM26_B1_EA1
            flow_label,
            r'TLM\d{2,}[_-]([UB]\d{1,})[_-]'
        ) AS label_unit,
        REGEXP_EXTRACT(
            flow_label,
            r'TLM\d{2,}[_-][UB]\d{1,}[_-]([A-Za-z]+[0-9]*)'
        ) AS label_activity_code,
        inserted_at,
        updated_at,
        bq_inserted_at
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
),
course_candidates AS (

    SELECT
        messages.id,
        crm_courses.course_id,
        CASE
            WHEN messages.legacy_course_id = crm_courses.course_id THEN 1
            WHEN STRPOS(
                messages.flow_label,
                crm_courses.course_id
            ) > 0 THEN 2
            WHEN crm_courses.course_area IS NOT NULL
            AND crm_courses.course_level IS NOT NULL
            AND STRPOS(
                messages.flow_label,
                crm_courses.course_area
            ) > 0
            AND STRPOS(
                messages.flow_label,
                crm_courses.course_level
            ) > 0 THEN 3
            ELSE 99
        END AS match_rank,
        LENGTH(crm_courses.course_id) AS course_id_length,
        crm_courses.enrollment_rows
    FROM
        messages
        INNER JOIN crm_courses
        ON messages.legacy_course_id = crm_courses.course_id
        OR STRPOS(
            messages.flow_label,
            crm_courses.course_id
        ) > 0
        OR (
            crm_courses.course_area IS NOT NULL
            AND crm_courses.course_level IS NOT NULL
            AND STRPOS(
                messages.flow_label,
                crm_courses.course_area
            ) > 0
            AND STRPOS(
                messages.flow_label,
                crm_courses.course_level
            ) > 0
        )
    QUALIFY ROW_NUMBER() over (
        PARTITION BY messages.id
        ORDER BY
            match_rank,
            course_id_length DESC,
            enrollment_rows DESC,
            course_id
    ) = 1
),
deduplicated_messages AS (

    SELECT
        messages.*,
        ROW_NUMBER() over (
            PARTITION BY messages.id
            ORDER BY
                messages.bq_inserted_at DESC
        ) AS row_no
    FROM
        messages
)
SELECT
    deduplicated_messages.id,
    deduplicated_messages.profile_id,
    deduplicated_messages.contact_phone,
    deduplicated_messages.flow_label,
    deduplicated_messages.message_type,
    deduplicated_messages.activity_status,
    COALESCE(
        deduplicated_messages.legacy_course_id,
        course_candidates.course_id
    ) AS course_id,
    deduplicated_messages.batch_ids [SAFE_OFFSET(0)] AS enrolled_batch_id,
    COALESCE(
        deduplicated_messages.batch_ids [SAFE_OFFSET(1)],
        deduplicated_messages.batch_ids [SAFE_OFFSET(0)]
    ) AS batch_id,
    COALESCE(
        deduplicated_messages.legacy_unit,
        deduplicated_messages.label_unit
    ) AS unit,
    COALESCE(
        deduplicated_messages.legacy_activity,
        IF(
            deduplicated_messages.label_activity_code IS NOT NULL,
            CONCAT(
                'Activity ',
                deduplicated_messages.label_activity_code
            ),
            NULL
        )
    ) AS activity,
    deduplicated_messages.inserted_at,
    deduplicated_messages.updated_at,
    deduplicated_messages.bq_inserted_at,
    deduplicated_messages.row_no,
    -- Categorizing activities into "student", "pretest" and "engagement" (TAPs logic)
    -- "student" : activities in unit starting with 'U'
    -- "pretest" : activities in unit starting with 'B' and having activity no 5
    -- "engagement" : activities in unit starting with 'B' and having activity no != 5
    CASE
        WHEN COALESCE(
            deduplicated_messages.legacy_unit,
            deduplicated_messages.label_unit
        ) LIKE 'U%' THEN "student"
        WHEN COALESCE(
            deduplicated_messages.legacy_unit,
            deduplicated_messages.label_unit
        ) LIKE 'B%'
        AND COALESCE(
            deduplicated_messages.legacy_activity,
            CONCAT(
                'Activity ',
                deduplicated_messages.label_activity_code
            )
        ) = 'Activity 5' THEN "pretest"
        WHEN COALESCE(
            deduplicated_messages.legacy_unit,
            deduplicated_messages.label_unit
        ) LIKE 'B%'
        AND COALESCE(
            deduplicated_messages.legacy_activity,
            CONCAT(
                'Activity ',
                deduplicated_messages.label_activity_code
            )
        ) != 'Activity 5' THEN "engagement"
        ELSE ''
    END AS activity_type,
    REGEXP_SUBSTR(
        COALESCE(
            deduplicated_messages.legacy_activity,
            CONCAT(
                'Activity ',
                deduplicated_messages.label_activity_code
            )
        ),
        '[0-9]+',
        1,
        1
    ) AS activity_no,
    REGEXP_SUBSTR(
        COALESCE(
            deduplicated_messages.legacy_unit,
            deduplicated_messages.label_unit
        ),
        '[0-9]+',
        1,
        1
    ) AS unit_no,
FROM
    deduplicated_messages
    LEFT JOIN course_candidates
    ON deduplicated_messages.id = course_candidates.id
WHERE
    row_no = 1
    AND activity_status IN (
        'Activity_Submission',
        'Activity_Sent',
        'Activity_Access'
    ) -- we dont care about messages that dont have these statuses
