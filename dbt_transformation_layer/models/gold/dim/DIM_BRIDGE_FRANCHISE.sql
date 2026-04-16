SELECT
    FK_GAME         AS "Game ID",
    FK_FRANCHISE    AS "Franchise ID"
FROM {{ ref('CLEAN_BRIDGE_FRANCHISE') }}