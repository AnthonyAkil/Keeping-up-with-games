SELECT 
    id          AS "Franchise ID",
    name        AS "Franchise name",
    TO_TIMESTAMP(
        created_at
    )::DATE     AS "Date franchise created"     -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_FRANCHISE') }} 