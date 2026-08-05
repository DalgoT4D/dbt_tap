{% test duplicate_messages(
    model
) %}
SELECT
    *
FROM
    {{ model }}
QUALIFY ROW_NUMBER() over (
        PARTITION BY phone,
        enrollment_id,
        msg_profile_id,
        course_id,
        batch_id,
        unit,
        activity,
        activity_status
        ORDER BY
            bq_inserted_at,
            message_id
    ) > 1 {% endtest %}
