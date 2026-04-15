SELECT 
    id                 AS "Game ID",
    name               AS "Game name",
    game_type          AS "Game type ID",
    total_rating       AS "Average rating",
    CASE
        WHEN total_rating_count = 0 
        AND total_rating > 0 AND total_rating < 95 -- A rating higher than 95 proved to have an actual review
            THEN 1
        WHEN total_rating_count > 0
        AND total_rating = 0
            THEN 0
        ELSE ZEROIFNULL(total_rating_count)        -- NULL reviews is synonymous to no reviews
    END                AS "Number of reviews",
    ZEROIFNULL(hypes)  AS "Wishlist count",        -- NULL wishlists is synonymous to no wishlists
    TO_TIMESTAMP(
        first_release_date
    )::DATE     AS "Date game initial release"     -- Converting the UNIX timestamp to GMT date

FROM {{ source('IGDB', 'RAW_GAMES') }} 
GROUP BY ALL                                       -- Handles duplicate, identical rows within source data