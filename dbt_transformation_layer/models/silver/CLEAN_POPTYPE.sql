SELECT 
    ID,
    NAME
FROM {{ source('IGDB', 'RAW_POPTYPE') }} 