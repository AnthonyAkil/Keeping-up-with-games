SELECT
    FK_GAME         AS "Game ID",
    FK_GENRE        AS "Genre ID"
FROM {{ ref('CLEAN_BRIDGE_GENRE') }}