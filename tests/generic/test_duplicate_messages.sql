{% test duplicate_messages(
    model,
    column_name
) %}
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
    ) AS row_no
FROM
    {{ model }}
WHERE
    row_no > 1 {% endtest %}
