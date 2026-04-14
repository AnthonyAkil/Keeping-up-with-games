SELECT 
    id          AS "Genre ID",
    name        AS "Genre name",
    TO_TIMESTAMP(
        created_at
    )::DATE     AS "Date genre created"     -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_GENRE') }} 