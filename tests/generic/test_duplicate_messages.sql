{% test duplicate_messages(
    model,
    column_name
) %}
SELECT
    phone,
    enrollment_id,
    msg_profile_id,
    course_id,
    batch_id,
    unit,
    activity,
    activity_status
FROM
    {{ model }}
GROUP BY
    phone,
    enrollment_id,
    msg_profile_id,
    course_id,
    batch_id,
    unit,
    activity,
    activity_status
HAVING
    COUNT(*) > 1 {% endtest %}
