SELECT
    FK_GAME         AS "Game ID",
    FK_FRANCHSE     AS "Franchise ID"
FROM {{ ref('CLEAN_BRIDGE_FRANCHISE') }}