SELECT 
    ID              AS "Platform ID",
    NAME            AS "Platform name",
    DATE_CREATED_AT AS "Date platform created"
FROM {{ ref('CLEAN_PLATFORM') }} 