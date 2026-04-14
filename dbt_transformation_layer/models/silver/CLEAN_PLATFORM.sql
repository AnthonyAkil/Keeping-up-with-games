SELECT 
    id          AS "Platform ID",
    name        AS "Platform name",
    TO_TIMESTAMP(
        created_at
    )::DATE     AS "Date platform created"     -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_PLATFORM') }} 