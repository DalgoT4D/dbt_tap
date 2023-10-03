{{ config(
    materialized = "incremental",
    schema = "intermediate",
    unique_key = "name"
) }}

SELECT
    *
FROM
    {{ source(
        'crm',
        'tabEnrollment'
    ) }}

{% if is_incremental() %}
WHERE
    modified > (
        SELECT
            MAX(modified)
        FROM
            {{ this }}
    )
{% endif %}
