{{ config(
    materialized = "incremental",
    unique_key = ["phone", "enrollment_id", "msg_profile_id", "course_id", "batch_id", "student_id", "unit", "activity", "activity_status"]
) }}
-- this model merges the frappe enrollment data with glific messages table on phone, course, batch, profile_id (if available)
-- incremental model works on processing the messages table. Only new records are processed every day instead of all of them
WITH enrollments AS (

    SELECT
        *,
        RIGHT(
            REGEXP_REPLACE(
                CAST(
                    phone AS STRING
                ),
                r'[^0-9]',
                ''
            ),
            10
        ) AS phone_key
    FROM
        {{ ref("student_enrollments") }}
),
messages AS (

    SELECT
        *,
        RIGHT(
            REGEXP_REPLACE(
                CAST(
                    contact_phone AS STRING
                ),
                r'[^0-9]',
                ''
            ),
            10
        ) AS contact_phone_key
    FROM
        {{ ref("glific_messages_deserialize") }}
),
candidate_enrollment_messages AS (

    SELECT
        enrollments.student_id,
        enrollments.student_name,
        enrollments.enrollment_id,
        enrollments.phone,
        enrollments.profile_id,
        enrollments.gender,
        enrollments.grade,
        enrollments.course_id,
        enrollments.course_name1,
        enrollments.course_name2,
        enrollments.batch_id,
        enrollments.batch_start_date,
        enrollments.batch_end_date,
        enrollments.batch_title,
        enrollments.batch_year,
        enrollments.school_id,
        enrollments.school_name,
        enrollments.school_type,
        enrollments.school_city AS city,
        messages.id AS message_id,
        messages.activity_status,
        messages.message_type,
        messages.activity_type,
        messages.profile_id AS msg_profile_id,
        messages.inserted_at,
        messages.bq_inserted_at,
        messages.updated_at,
        CAST(
            messages.activity_no AS INT
        ) AS activity_no,
        CAST(
            messages.unit_no AS INT
        ) AS unit_no,
        messages.unit,
        messages.activity,
        CASE
            WHEN messages.activity_status = 'Activity_Sent' THEN 1
            ELSE 0
        END AS `sent`,
        CASE
            WHEN messages.activity_status = 'Activity_Access' THEN 1
            ELSE 0
        END AS `accessed`,
        CASE
            WHEN messages.activity_status = 'Activity_Submission' THEN 1
            ELSE 0
        END AS `submitted`,
        CASE
            WHEN enrollments.course_id = messages.course_id THEN 1
            ELSE 2
        END AS course_match_rank,
        COUNT(*) over (
            PARTITION BY messages.id
        ) AS message_candidate_count
    FROM
        enrollments
        INNER JOIN messages
        ON messages.contact_phone_key = enrollments.phone_key
        AND (
            messages.profile_id IS NULL
            OR enrollments.profile_id IS NULL
            OR enrollments.profile_id = CAST(
                messages.profile_id AS STRING
            )
        )
        AND (
            enrollments.course_id = messages.course_id
            OR (
                messages.course_id IS NULL
                AND enrollments.batch_year >= 2026
            )
        )
        AND (
            (
                messages.batch_id IS NOT NULL
                AND enrollments.batch_id = messages.batch_id -- legacy labels contain batch id
            )
            OR (
                messages.batch_id IS NULL
                AND SAFE_CAST(messages.inserted_at AS DATE) BETWEEN enrollments.batch_start_date
                AND enrollments.batch_end_date
            )
        )
    WHERE
        messages.activity_status IS NOT NULL

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
),
merged_enrollment_messages AS (

    SELECT
        * EXCEPT (
            course_match_rank,
            message_candidate_count
        )
    FROM
        candidate_enrollment_messages
    WHERE
        course_match_rank = 1
        OR message_candidate_count = 1
),
duplicated_merge AS (
    -- this removes the duplicate messages on parameters below
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY phone,
            enrollment_id,
            msg_profile_id,
            course_id,
            batch_id,
            unit,
            activity,
            activity_status
            ORDER BY
                bq_inserted_at
        ) AS row_no
    FROM
        merged_enrollment_messages
)
SELECT
    *
FROM
    duplicated_merge
WHERE
    row_no = 1
