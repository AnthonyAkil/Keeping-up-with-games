SELECT 
    ID              AS "Genre ID",
    NAME            AS "Genre name",
    DATE_CREATED_AT AS "Date genre created"
FROM {{ ref('CLEAN_GENRE') }} 