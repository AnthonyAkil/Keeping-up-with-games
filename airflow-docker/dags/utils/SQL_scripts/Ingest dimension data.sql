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

---> load using COPY INTO to retain reflection of source data):
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
--> Use Merge into pattern to uphold idempotent pipeline
MERGE INTO IGDB.GOLD.DIM_GAMEMODE AS target
USING (
    SELECT 
        id,
        name
    FROM IGDB.BRONZE.GAMEMODE_RAW
) AS source
ON target."Game mode ID" = source.id
WHEN MATCHED THEN
    UPDATE SET 
        target."Game mode name" = source.name
WHEN NOT MATCHED THEN
    INSERT (target."Game mode ID", target."Game mode name")
    VALUES (source.id, source.name);


MERGE INTO IGDB.GOLD.DIM_GENRE AS target
USING (
    SELECT 
        id,
        name
    FROM IGDB.BRONZE.GENRE_RAW
) AS source
ON target."Genre ID" = source.id
WHEN MATCHED THEN
    UPDATE SET 
        target."Genre name" = source.name
WHEN NOT MATCHED THEN
    INSERT (target."Genre ID", target."Genre name")
    VALUES (source.id, source.name);


MERGE INTO IGDB.GOLD.DIM_PLATFORM AS target
USING (
    SELECT  
        id,
        name
    FROM IGDB.BRONZE.PLATFORM_RAW
) AS source
ON target."Platform ID" = source.id
WHEN MATCHED THEN
    UPDATE SET
      target."Platform name" = source.name
WHEN NOT MATCHED THEN
    INSERT ("Platform ID", "Platform name")
    VALUES (source.id, source.name);


MERGE INTO IGDB.GOLD.DIM_FRANCHISE AS target
USING (
    SELECT
      id,
      name
    FROM IGDB.BRONZE.FRANCHISE_RAW
) AS source
ON target."Franchise ID" = source.id
WHEN MATCHED THEN
    UPDATE SET  
      target."Franchise name" = source.name
WHEN NOT MATCHED THEN
    INSERT (target."Franchise ID", target."Franchise name")
    VALUES (source.id, source.name);
    

MERGE INTO IDGB.GOLD.DIM_GAMETYPE AS target
USING (
    SELECT
      id,
      type
    FROM IGDB.BRONZE.GAMETYPE_RAW
) AS source
ON target."Game type ID" = source.id
WHEN MATCHED THEN 
    UPDATE target."Game type name" = source.type
WHEN NOT MATCHED THEN
    INSERT (target."Game type ID", target."Game type name")
    VALUES (source.id, source.type);