SELECT 
    id          AS "Game mode ID",
    name        AS "Game mode name",
    TO_TIMESTAMP(
        created_at
    )::DATE     AS "Date game mode created"     -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_GAMEMODE') }} 