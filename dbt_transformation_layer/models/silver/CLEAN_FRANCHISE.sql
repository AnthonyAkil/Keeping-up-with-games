SELECT 
    ID,
    NAME,
    TO_TIMESTAMP(
        created_at
    )::DATE     AS DATE_CREATED_AT    -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_FRANCHISE') }} 