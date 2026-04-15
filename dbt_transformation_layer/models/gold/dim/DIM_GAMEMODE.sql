SELECT 
    ID               AS "Game mode ID",
    NAME             AS "Game mode name",
    DATE_CREATED_AT  AS "Date game mode created"
FROM {{ ref('CLEAN_GAMEMODE') }} 