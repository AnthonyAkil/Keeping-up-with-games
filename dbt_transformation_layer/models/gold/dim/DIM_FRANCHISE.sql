SELECT 
    ID               AS "Franchise ID",
    NAME             AS "Franchise name",
    DATE_CREATED_AT  AS "Date franchise created"
FROM {{ ref('CLEAN_FRANCHISE') }} 