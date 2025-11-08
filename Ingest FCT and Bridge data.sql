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


---> load using initial file for testing purposes (not automation-ready):
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
TRUNCATE TABLE IGDB.GOLD.BRIDGE_GAMEMODE;
INSERT INTO IGDB.GOLD.BRIDGE_GAMEMODE
SELECT 
  raw_games.id AS "Game ID",
  unpacked.value::TINYINT AS "Game mode ID"
FROM IGDB.BRONZE.GAMES_RAW raw_games,
LATERAL FLATTEN(input => raw_games.game_modes) unpacked;

TRUNCATE TABLE IGDB.GOLD.BRIDGE_GENRE;
INSERT INTO IGDB.GOLD.BRIDGE_GENRE
SELECT 
  raw_games.id AS "Game ID",
  unpacked.value::TINYINT AS "Genre ID"
FROM IGDB.BRONZE.GAMES_RAW raw_games,
LATERAL FLATTEN(input => raw_games.genres) unpacked;

TRUNCATE TABLE IGDB.GOLD.BRIDGE_FRANCHISE;
INSERT INTO IGDB.GOLD.BRIDGE_FRANCHISE
SELECT 
  raw_games.id AS "Game ID",
  unpacked.value::TINYINT AS "Franchise ID"
FROM IGDB.BRONZE.GAMES_RAW raw_games,
LATERAL FLATTEN(input => raw_games.franchises) unpacked;

TRUNCATE TABLE IGDB.GOLD.BRIDGE_PLATFORM;
INSERT INTO IGDB.GOLD.BRIDGE_PLATFORM
SELECT 
  raw_games.id AS "Game ID",
  unpacked.value::TINYINT AS "Platform ID"
FROM IGDB.BRONZE.GAMES_RAW raw_games,
LATERAL FLATTEN(input => raw_games.platforms) unpacked;



TRUNCATE TABLE IGDB.GOLD.GAMES;
INSERT INTO IGDB.GOLD.GAMES (
    "Game ID",               
    "Game name",         
    "Initial release date",
    "Game type ID",                  
    "Total rating",        
    "Total rating count",                           
    "Hypes" 
)
SELECT 
    id,
    name,
    TO_TIMESTAMP(first_release_date)::DATE, -- Convert Unix timestamp into date
    game_type,
    total_rating,
    total_rating_count,
    hypes
FROM IGDB.BRONZE.GAMES_RAW;
