SELECT
    FK_GAME         AS "Game ID",
    FK_PLATFORM     AS "Platform ID"
FROM {{ ref('CLEAN_BRIDGE_PLATFORM') }}