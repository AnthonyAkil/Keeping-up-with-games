SELECT 
    clean_games.ID                          AS "Game ID",
    clean_games.NAME                        AS "Game name",
    clean_games.FK_GAME_TYPE                AS "Game type ID",
    clean_status.STATUS                     AS "Status",
    clean_games.AVERAGE_RATING              AS "Average rating",
    clean_games.NUM_OF_REVIEWS              AS "Number of reviews",
    clean_games.WISHLIST_COUNT              AS "Wishlist count",
    clean_games.DATE_FIRST_RELEASE          AS "Date game initial release"
FROM {{ ref('CLEAN_GAMES') }}   AS clean_games
LEFT JOIN {{ ref('CLEAN_GAMESTATUS')}} AS clean_status
    ON clean_games.FK_STATUS = clean_status.ID