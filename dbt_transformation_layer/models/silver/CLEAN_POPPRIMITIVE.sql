SELECT 
    ID,
    game_id         AS FK_GAME,
    popularity_type AS FK_POPULARITY_TYPE,
    value           AS POPULARITY_SCORE,
    TO_TIMESTAMP(
        calculated_at
    )::DATE     AS DATE_MEASURED_AT,              -- Converting the UNIX timestamp to GMT date
FROM {{ source('IGDB', 'RAW_POPPRIMITIVE') }}