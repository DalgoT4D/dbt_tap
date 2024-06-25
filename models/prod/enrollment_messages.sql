{{ config(
    materialized = "incremental",
    unique_key = ["phone", "enrollment_id", "msg_profile_id", "course_id", "batch_id", "student_id", "unit", "activity", "activity_status"]
) }}
-- this model merges the frappe enrollment data with glific messages table on phone, course, batch, profile_id (if available)
-- incremental model works on processing the messages table. Only new records are processed every day instead of all of them
WITH merged_enrollment_messages AS (

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
    FROM
        {{ ref("student_enrollments") }} AS enrollments
        INNER JOIN {{ ref("glific_messages_deserialize") }} AS messages
        ON (
            (
                messages.profile_id IS NULL
                AND CONCAT(
                    '91',
                    enrollments.phone
                ) = messages.contact_phone
                AND enrollments.course_id = messages.course_id
                AND enrollments.batch_id = messages.enrolled_batch_id
            )
            OR (
                messages.profile_id IS NOT NULL
                AND enrollments.profile_id = CAST(
                    messages.profile_id AS STRING
                )
                AND CONCAT(
                    '91',
                    enrollments.phone
                ) = messages.contact_phone
                AND enrollments.course_id = messages.course_id
                AND enrollments.batch_id = messages.enrolled_batch_id
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
