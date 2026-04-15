SELECT
    raw_games.id            AS FK_GAME,
    unpacked.value::TINYINT AS FK_GAME_MODE
FROM {{ source('IGDB', 'RAW_GAMES') }}          AS raw_games,
LATERAL FLATTEN(input => raw_games.game_modes)  AS unpacked