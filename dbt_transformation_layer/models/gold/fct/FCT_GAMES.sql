SELECT 
    ID                          AS "Game ID",
    NAME                        AS "Game name",
    FK_GAME_TYPE                AS "Game type ID",
    AVERAGE_RATING              AS "Average rating",
    NUM_OF_REVIEWS              AS "Number of reviews",
    WISHLIST_COUNT              AS "Wishlist count",
    DATE_FIRST_RELEASE          AS "Date game initial release"
FROM {{ ref('CLEAN_GAMES') }} 