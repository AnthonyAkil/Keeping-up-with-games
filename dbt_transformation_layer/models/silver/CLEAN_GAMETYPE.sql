SELECT 
    id          AS "Game type ID",
    type        AS "Game type name",
    TO_TIMESTAMP(
        created_at
    )::DATE     AS "Date game type created"     -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_GAMETYPE') }} 