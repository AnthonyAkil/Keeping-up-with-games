SELECT 
    ID,
    NAME,
    game_type          AS FK_GAME_TYPE,
    total_rating       AS AVERAGE_RATING,
    CASE
        WHEN total_rating_count = 0 
        AND total_rating > 0 AND total_rating < 95 -- A rating higher than 95 proved to have an actual review
            THEN 1
        WHEN total_rating_count > 0
        AND total_rating = 0
            THEN 0
        ELSE ZEROIFNULL(total_rating_count)        -- NULL reviews is synonymous to no reviews
    END                AS NUM_OF_REVIEWS,
    ZEROIFNULL(hypes)  AS WISHLIST_COUNT,          -- NULL wishlists is synonymous to no wishlists
    game_status        AS FK_STATUS,
    TO_TIMESTAMP(
        first_release_date
    )::DATE     AS DATE_FIRST_RELEASE              -- Converting the UNIX timestamp to GMT date

FROM {{ source('IGDB', 'RAW_GAMES') }} 
GROUP BY ALL                                       -- Handles duplicate, identical rows within source data