{{
  config(
    materialized = 'incremental',
    on_schema_change ='fail',
    incremental_strategy ='append'
    )
}}

SELECT 
    id                 AS "Game ID",
    name               AS "Game name",
    game_type          AS "Game type ID",
    total_rating       AS "Rating average",
    total_rating_count AS "Rating count",
    hypes              AS "Wishlist count",
    TO_TIMESTAMP(
        first_release_date
    )::DATE     AS "Date game initial release"     -- Converting the UNIX timestamp to GMT date

FROM {{ source('IGDB', 'RAW_GAMES') }} 
{% if is_incremental() %}
    WHERE TO_TIMESTAMP(updated_at) > (SELECT MAX(TO_TIMESTAMP(updated_at)) FROM {{ source('IGDB', 'RAW_GAMES') }}  ) 
{% endif %}