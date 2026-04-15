SELECT 
    ID               AS "Game type ID",
    TYPE             AS "Game type name",
    DATE_CREATED_AT  AS "Date game type created"
FROM {{ ref('CLEAN_GAMETYPE') }} 