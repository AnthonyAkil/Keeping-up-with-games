SELECT
    ID,
    STATUS
FROM {{ source('IGDB', 'RAW_GAMESTATUS') }} 