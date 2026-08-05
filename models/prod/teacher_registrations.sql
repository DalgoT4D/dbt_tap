{{ config(
    materialized = "table",
) }}

SELECT
    teacher.name AS teacher_id,
    COALESCE(
        NULLIF(
            teacher.teachername,
            ''
        ),
        NULLIF(
            teacher.teachname,
            ''
        ),
        NULLIF(
            CONCAT(
                COALESCE(
                    teacher.first_name,
                    ''
                ),
                ' ',
                COALESCE(
                    teacher.last_name,
                    ''
                )
            ),
            ' '
        ),
        teacher.name
    ) AS teacher_name,
    teacher.first_name,
    teacher.last_name,
    COALESCE(
        NULLIF(
            teacher.phone,
            ''
        ),
        NULLIF(
            teacher.phone_number,
            ''
        )
    ) AS phone,
    RIGHT(
        REGEXP_REPLACE(
            CAST(
                COALESCE(
                    NULLIF(
                        teacher.phone,
                        ''
                    ),
                    NULLIF(
                        teacher.phone_number,
                        ''
                    )
                ) AS STRING
            ),
            r'[^0-9]',
            ''
        ),
        10
    ) AS phone_key,
    teacher.email_id,
    teacher.gender,
    teacher.language,
    teacher.state,
    teacher.teacher_role,
    COALESCE(
        NULLIF(
            teacher.teacher_batch,
            ''
        ),
        NULLIF(
            teacher.teacher_batch_,
            ''
        )
    ) AS teacher_batch_id,
    teacher.course_level,
    COALESCE(
        NULLIF(
            teacher.school_id,
            ''
        ),
        NULLIF(
            teacher.school,
            ''
        )
    ) AS school_id,
    school.name1 AS school_name,
    school.city AS school_city,
    school.model AS school_model,
    school.type AS school_type,
    teacher.glific_id,
    teacher.glific_sync_status,
    teacher.creation AS created_at,
    SAFE_CAST(
        teacher.creation AS DATE
    ) AS created_date,
    DATE_TRUNC(
        SAFE_CAST(
            teacher.creation AS DATE
        ),
        MONTH
    ) AS created_month,
    EXTRACT(
        YEAR
        FROM
            SAFE_CAST(
                teacher.creation AS DATE
            )
    ) AS created_year,
    teacher.modified AS modified_at
FROM
    {{ source(
        'crm',
        'tabTeacher'
    ) }} AS teacher
    LEFT JOIN {{ source(
        'crm',
        'tabSchool'
    ) }} AS school
    ON COALESCE(
        NULLIF(
            teacher.school_id,
            ''
        ),
        NULLIF(
            teacher.school,
            ''
        )
    ) = school.name
