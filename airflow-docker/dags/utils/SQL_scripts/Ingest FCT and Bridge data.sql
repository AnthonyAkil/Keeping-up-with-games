"""
This SQL-script is used to ingest the data into the corresponding FACT and BRIDGE tables and prepare the data for analysis.

"""
---> set Role Context
USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

USE DATABASE IGDB;




-- #############################################################
-- BRONZE TABLES
-- #############################################################


---> load using COPY INTO to retain reflection of source data):
TRUNCATE TABLE IGDB.BRONZE.GAMES_RAW;
COPY INTO IGDB.BRONZE.GAMES_RAW
  FROM @IGDB.BRONZE.S3_stage
  FILE_FORMAT = (TYPE = 'parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILES = ('igdb_api_data_20251107.parquet');



-- #############################################################
-- GOLD TABLES
-- #############################################################

-- Insert data into the bridge tables by unpacking raw source data fields:
MERGE INTO IGDB.GOLD.BRIDGE_GAMEMODE AS target
USING (
    SELECT
      raw_games.id AS game_id,
      unpacked.value::TINYINT AS game_mode_id
    FROM IGDB.BRONZE.GAMES_RAW AS raw_games
    LATERAL FLATTEN(input => raw_games.game_modes) AS unpacked
) AS source
ON target."Game ID" = source.game_id
  AND target."Game mode ID" AS source.game_mode id
WHEN NOT MATCHED THEN
  INSERT SET (target."Game ID", target."Game mode ID")
  VALUES (source.game_id, source.game_mode_id);


MERGE INTO IGDB.GOLD.BRIDGE_GENRE AS target
USING (
    SELECT
        raw_games.id AS game_id,
        unpacked.value::TINYINT AS genre_id
    FROM IGDB.BRONZE.GAMES_RAW AS raw_games
    LATERAL FLATTEN(input => raw_games.genres) AS unpacked
) AS source
ON target."Game id" = source.game_id
  AND target."Genre ID" = source.genre_id
WHEN NOT MATCHED THEN
  INSERT SET (target."Game ID", target."Genre ID")
  VALUES (source.game_id, source.genre_id);


MERGE INTO IGDB.GOLD.BRIDGE_FRANCHISE AS target
USING (
    SELECT
        raw_games.id AS game_id,
        unpacked.value::TINYINT AS franchise_id
    FROM IGDB.BRONZE.GAMES_RAW AS raw_games
    LATERAL FLATTEN(input => raw_games.genres) AS unpacked
) AS source
ON target."Game id" = source.game_id
  AND target."Franchise ID" = source.franchise_id
WHEN NOT MATCHED THEN
  INSERT SET (target."Game ID", target."Franchise ID")
  VALUES (source.game_id, source.franchise_id)


MERGE INTO IGDB.GOLD.BRIDGE_PLATFORM AS target
USING (
    SELECT
        raw_games.id AS game_id,
        unpacked.value::TINYINT AS platform_id
    FROM IGDB.BRONZE.GAMES_RAW AS raw_games
    LATERAL FLATTEN(input => raw_games.platforms) AS unpacked
) AS source
ON target."Game id" = source.game_id
  AND target."Platform ID" = source.platform_id
WHEN NOT MATCHED THEN
  INSERT SET (target."Game ID", target."Platform ID")
  VALUES (source.game_id, source.platform_id)


MERGE INTO IGDB.GOLD.BRIDGE_PLATFORM AS target
USING (
    SELECT
        raw_games.id AS game_id,
        unpacked.value::TINYINT AS platform_id
    FROM IGDB.BRONZE.GAMES_RAW AS raw_games
    LATERAL FLATTEN(input => raw_games.platforms) AS unpacked
) AS source
ON target."Game id" = source.game_id
  AND target."Platform ID" = source.platform_id
WHEN NOT MATCHED THEN
  INSERT SET (target."Game ID", target."Platform ID")
  VALUES (source.game_id, source.platform_id)


MERGE INTO IGDB.GOLD.GAMES AS target
USING (
    SELECT 
        id,
        name,
        TO_TIMESTAMP(first_release_date)::DATE AS release_date,
        game_type,
        total_rating,
        total_rating_count,
        hypes
    FROM IGDB.BRONZE.GAMES_RAW
) AS source
ON target."Game ID" = source.id
WHEN MATCHED THEN
    UPDATE SET 
        target."Game name" = source.name,
        target."Initial release date" = source.release_date,
        target."Game type ID" = source.game_type,
        target."Total rating" = source.total_rating,
        target."Total rating count" = source.total_rating_count,
        target."Hypes" = source.hypes
WHEN NOT MATCHED THEN
    INSERT (
        "Game ID",
        "Game name",
        "Initial release date",
        "Game type ID",
        "Total rating",
        "Total rating count",
        "Hypes"
    )
    VALUES (
        source.id,
        source.name,
        source.release_date,
        source.game_type,
        source.total_rating,
        source.total_rating_count,
        source.hypes
    );
