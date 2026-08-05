{{ config(
    materialized = "table",
) }}

SELECT
    created_month,
    teacher_batch_id,
    COALESCE(
        NULLIF(
            state,
            ''
        ),
        'Unknown'
    ) AS state,
    COALESCE(
        NULLIF(
            teacher_role,
            ''
        ),
        'Unknown'
    ) AS teacher_role,
    COUNT(DISTINCT teacher_id) AS teachers_registered,
    COUNT(DISTINCT school_id) AS schools,
    COUNTIF(glific_id IS NOT NULL AND glific_id != '') AS teachers_with_glific_id,
    SAFE_DIVIDE(
        COUNTIF(glific_id IS NOT NULL AND glific_id != ''),
        COUNT(DISTINCT teacher_id)
    ) AS teacher_glific_sync_rate
FROM
    {{ ref('teacher_registrations') }}
WHERE
    created_year = 2026
GROUP BY
    1,
    2,
    3,
    4
