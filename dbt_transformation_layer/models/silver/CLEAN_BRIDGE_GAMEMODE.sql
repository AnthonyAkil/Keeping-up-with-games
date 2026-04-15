SELECT
    raw_games.id AS game_id,
    unpacked.value::TINYINT AS game_mode_id
FROM {{ source('IGDB', 'RAW_GAMES') }}          AS raw_games,
LATERAL FLATTEN(input => raw_games.game_modes)  AS unpacked