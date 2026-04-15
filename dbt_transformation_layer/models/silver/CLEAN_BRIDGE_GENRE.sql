SELECT
    raw_games.id AS game_id,
    unpacked.value::TINYINT AS genre_id
FROM {{ source('IGDB', 'RAW_GAMES') }}     AS raw_games,
LATERAL FLATTEN(input => raw_games.genres) AS unpacked