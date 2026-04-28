SELECT 
    ID                  AS "Popularity ID",
    FK_GAME             AS "Game ID",
    FK_POPULARITY_TYPE  AS "Popularity type ID",
    POPULARITY_SCORE    AS "Popularity score",
    DATE_MEASURED_AT    AS "Date popularity measured at"
FROM {{ ref('CLEAN_POPPRIMITIVE') }}