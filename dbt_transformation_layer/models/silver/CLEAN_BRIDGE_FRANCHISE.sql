{{
  config(
    materialized = 'incremental',
    on_schema_change ='fail',
    incremental_strategy ='append'
    )
}}

SELECT
    raw_games.id AS game_id,
    unpacked.value::TINYINT AS franchise_id
FROM {{ source('IGDB', 'RAW_GAMES') }}           AS raw_games,
LATERAL FLATTEN(input => raw_games.franchises)   AS unpacked

{% if is_incremental() %}
    WHERE TO_TIMESTAMP(updated_at) > (SELECT MAX(TO_TIMESTAMP(updated_at)) FROM {{ source('IGDB', 'RAW_GAMES') }}  ) 
{% endif %}