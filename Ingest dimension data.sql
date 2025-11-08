"""
This SQL-script is used to ingest the data into the corresponding DIMENSION tables and prepare the data for analysis.

"""
---> set Role Context
USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

USE DATABASE IGDB;




-- #############################################################
-- BRONZE TABLES
-- #############################################################


TRUNCATE TABLE IGDB.BRONZE.GAMEMODE_RAW;
COPY INTO IGDB.BRONZE.GAMEMODE_RAW
  FROM @IGDB.BRONZE.S3_stage
  FILE_FORMAT = (TYPE = 'parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILES = ('game_modes_20251105.parquet');

TRUNCATE TABLE IGDB.BRONZE.GENRE_RAW;
COPY INTO IGDB.BRONZE.GENRE_RAW
  FROM @IGDB.BRONZE.S3_stage
  FILE_FORMAT = (TYPE = 'parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILES = ('genres_20251105.parquet');

TRUNCATE TABLE IGDB.BRONZE.PLATFORM_RAW;
COPY INTO IGDB.BRONZE.PLATFORM_RAW
  FROM @IGDB.BRONZE.S3_stage
  FILE_FORMAT = (TYPE = 'parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILES = ('platforms_20251105.parquet');

TRUNCATE TABLE IGDB.BRONZE.FRANCHISE_RAW;
COPY INTO IGDB.BRONZE.FRANCHISE_RAW
  FROM @IGDB.BRONZE.S3_stage
  FILE_FORMAT = (TYPE = 'parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILES = ('franchises_20251105.parquet');

TRUNCATE TABLE IGDB.BRONZE.GAMETYPE_RAW;
COPY INTO IGDB.BRONZE.GAMETYPE_RAW
  FROM @IGDB.BRONZE.S3_stage
  FILE_FORMAT = (TYPE = 'parquet')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILES = ('game_types_20251108.parquet');




-- #############################################################
-- GOLD TABLES
-- #############################################################

--> Ingest data from raw tables into Dimension tables
TRUNCATE TABLE IGDB.GOLD.DIM_GAMEMODE;
INSERT INTO IGDB.GOLD.DIM_GAMEMODE (
    "Game mode ID",
    "Game mode name"
)
SELECT 
    id,
    name
FROM IGDB.BRONZE.GAMEMODE;

TRUNCATE TABLE IGDB.GOLD.DIM_GENRE;
INSERT INTO IGDB.GOLD.DIM_GENRE (
    "Genre ID",
    "Genre name"
)
SELECT 
    id,
    name
FROM IGDB.BRONZE.GENRE_RAW;

TRUNCATE TABLE IGDB.GOLD.DIM_PLATFORM;
INSERT INTO IGDB.GOLD.DIM_PLATFORM (
    "Platform ID",
    "Platform name"
)
SELECT 
    id,
    name
FROM IGDB.BRONZE.PLATFORM_RAW;

TRUNCATE TABLE IGDB.GOLD.DIM_FRANCHISE;
INSERT INTO IGDB.GOLD.DIM_FRANCHISE (
    "Franchise ID",
    "Franchise name"
)
SELECT 
    id,
    name
FROM IGDB.BRONZE.FRANCHISE_RAW;

TRUNCATE TABLE IGDB.GOLD.DIM_GAMETYPE;
INSERT INTO IGDB.GOLD.DIM_GAMETYPE (
    "Game type ID",
    "Game type name"
)
SELECT 
    id,
    type
FROM IGDB.BRONZE.GAMETYPE_RAW;