WITH input_bayesian_average AS (
    SELECT
        AVG(AVERAGE_RATING)       AS global_mean,
        MEDIAN(NUM_OF_REVIEWS)    AS confidence_threshold -- Data is skewed, which median is less sensitive to
    FROM {{ ref('CLEAN_GAMES') }}
    WHERE AVERAGE_RATING IS NOT NULL
      AND NUM_OF_REVIEWS > 0
)

SELECT 
    clean_games.ID                          AS "Game ID",
    clean_games.NAME                        AS "Game name",
    clean_games.FK_GAME_TYPE                AS "Game type ID",
    clean_status.STATUS                     AS "Status",
    clean_games.AVERAGE_RATING              AS "Average rating",
    clean_games.NUM_OF_REVIEWS              AS "Number of reviews",
    clean_games.WISHLIST_COUNT              AS "Wishlist count",
    clean_games.DATE_FIRST_RELEASE          AS "Date game initial release",
    ROUND(
        (weighted_input.confidence_threshold * weighted_input.global_mean + clean_games.AVERAGE_RATING * clean_games.NUM_OF_REVIEWS)
        / (weighted_input.confidence_threshold + clean_games.NUM_OF_REVIEWS),
    2)                                      AS "Weighted rating"        -- Abstracts the weighting method from the end users
FROM {{ ref('CLEAN_GAMES') }}   AS clean_games
LEFT JOIN {{ ref('CLEAN_GAMESTATUS')}} AS clean_status
    ON clean_games.FK_STATUS = clean_status.ID
CROSS JOIN input_bayesian_average AS weighted_input