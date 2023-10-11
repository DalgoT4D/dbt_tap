{{ config(
    materialized = "incremental",
    schema = "prod",
    unique_key = ["message_id", "enrollment_id"]
) }}
-- this model merges the frappe enrollment data with glific messages table on phone, course, batch, profile_id (if available)
-- incremental model on this considers two cases and unions them
----- when a new message(s) comes in
----- when a new enrollment(s) comes in
WITH merged_enrollment_messages AS (

    SELECT
        enrollments.student_id,
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
        enrollments.school_id,
        enrollments.school_name,
        enrollments.school_type,
        enrollments.school_city AS city,
        enrollments.last_sync_time,
        messages.id AS message_id,
        messages.activity_status,
        messages.message_type,
        messages.activity_type,
        messages.profile_id AS msg_profile_id,
        messages.inserted_at,
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
    FROM
        {{ ref("student_enrollments") }} AS enrollments
        INNER JOIN {{ ref("glific_messages_deserialize") }} AS messages
        ON (
            CONCAT(
                '91',
                enrollments.phone
            ) = messages.contact_phone
            AND enrollments.course_id = messages.course_id
            AND enrollments.batch_id = messages.batch_id
            OR enrollments.profile_id = CAST(
                messages.profile_id AS STRING
            )
        )
    WHERE
        messages.activity_status IS NOT NULL

{% if is_incremental() %}
AND (
    inserted_at > (
        SELECT
            MAX(inserted_at)
        FROM
            {{ this }}
    )
)
{% endif %}
UNION ALL
SELECT
    enrollments.student_id,
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
    enrollments.school_id,
    enrollments.school_name,
    enrollments.school_type,
    enrollments.school_city AS city,
    enrollments.last_sync_time,
    messages.id AS message_id,
    messages.activity_status,
    messages.message_type,
    messages.activity_type,
    messages.profile_id AS msg_profile_id,
    messages.inserted_at,
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
FROM
    {{ ref("student_enrollments") }} AS enrollments
    INNER JOIN {{ ref("glific_messages_deserialize") }} AS messages
    ON (
        CONCAT(
            '91',
            enrollments.phone
        ) = messages.contact_phone
        AND enrollments.course_id = messages.course_id
        AND enrollments.batch_id = messages.batch_id
        OR enrollments.profile_id = CAST(
            messages.profile_id AS STRING
        )
    )
WHERE
    messages.activity_status IS NOT NULL

{% if is_incremental() %}
AND (
    last_sync_time > (
        SELECT
            MAX(last_sync_time)
        FROM
            {{ this }}
    )
)
{% endif %}
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
                inserted_at
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
