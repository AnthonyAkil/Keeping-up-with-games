SELECT
    FK_GAME         AS "Game ID",
    FK_GAME_MODE    AS "Game mode ID"
FROM {{ ref('CLEAN_BRIDGE_GAMEMODE') }}