/*"""
This SQL-script is used to create the database, tables and Azure storage integration such that the data can be loaded in Snowflake.

"""*/
---> Set Role Context
USE ROLE ACCOUNTADMIN;

---> Set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> Create the Database
CREATE DATABASE IF NOT EXISTS IGDB
  COMMENT = 'This database contains the objects used to serve analysis of the IGDB API.';

USE DATABASE IGDB;

---> Create the Schema
CREATE SCHEMA IF NOT EXISTS IGDB.BRONZE
  COMMENT = 'Schema for tables loaded from Azure Blob Storage';

CREATE SCHEMA IF NOT EXISTS IGDB.GOLD
  COMMENT = 'Schema for production-ready tables ';

---> Create Azure Blob Storage integration
CREATE STORAGE INTEGRATION IGDB_AZURE_BLOB
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '15e1619c-3dcc-49be-a66a-e8353d255aec'
  STORAGE_ALLOWED_LOCATIONS = ('azure://storageigdbprojectrg.blob.core.windows.net/igdb-storage-container/*');

  
---> Retrieve the consent URL, navigate to it and accept the permission request:
DESC INTEGRATION IGDB_AZURE_BLOB;

/*
Grant Snowflake Access to the Storage Location via IAM -> role assignments
*/


---> Create external stage
CREATE STAGE IF NOT EXISTS IGDB.BRONZE.Blob_stage
  STORAGE_INTEGRATION = IGDB_AZURE_BLOB
  URL = 'azure://storageigdbprojectrg.blob.core.windows.net/igdb-storage-container/'
  FILE_FORMAT = (TYPE = 'parquet');




-- #############################################################
-- BRONZE TABLES
-- #############################################################

---> create the bronze table to land raw data
CREATE TABLE IF NOT EXISTS IGDB.BRONZE.GAMES_RAW (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255),         
    first_release_date INT,
    game_modes ARRAY,          
    game_type TINYINT,        
    genres ARRAY,   
    platforms ARRAY,           
    total_rating NUMBER(5,2),        
    total_rating_count INT,                  
    franchises ARRAY,
    hypes SMALLINT )
COMMENT = 'Table to be loaded from Blob Container raw data';


--> create raw tables used for dimensions
CREATE TABLE IF NOT EXISTS IGDB.BRONZE.GAMEMODE_RAW (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255)      
);

CREATE TABLE IF NOT EXISTS IGDB.BRONZE.GENRE_RAW (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255)      
);

CREATE TABLE IF NOT EXISTS IGDB.BRONZE.PLATFORM_RAW (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255)     
);
  
CREATE TABLE IF NOT EXISTS IGDB.BRONZE.FRANCHISE_RAW (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255)      
);

CREATE TABLE IF NOT EXISTS IGDB.BRONZE.GAMETYPE_RAW (
    id SMALLINT PRIMARY KEY,               
    type VARCHAR(255)      
);





-- #############################################################
-- GOLD TABLES
-- #############################################################

CREATE TABLE IF NOT EXISTS IGDB.GOLD.BRIDGE_GAMEMODE (
    "Game ID" SMALLINT,
    "Game mode ID" TINYINT
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.BRIDGE_GENRE (
    "Game ID" SMALLINT,
    "Genre ID" TINYINT
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.BRIDGE_FRANCHISE (
    "Game ID" SMALLINT,
    "Franchise ID" TINYINT
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.BRIDGE_PLATFORM (
    "Game ID" SMALLINT,
    "Platform ID" TINYINT
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.DIM_GAMEMODE (
    "Game mode ID" TINYINT PRIMARY KEY,
    "Game mode name" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.DIM_GENRE (
    "Genre ID" TINYINT PRIMARY KEY,
    "Genre name" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.DIM_PLATFORM (
    "Platform ID" TINYINT PRIMARY KEY,
    "Platform name" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.DIM_FRANCHISE (
    "Franchise ID" TINYINT PRIMARY KEY,
    "Franchise name" VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS IGDB.GOLD.DIM_GAMETYPE (
    "Game type ID" TINYINT PRIMARY KEY,
    "Game type name" VARCHAR(255)
);


---> create the Fact table to land each game
CREATE TABLE IF NOT EXISTS IGDB.GOLD.GAMES (
    "Game ID" SMALLINT PRIMARY KEY,               
    "Game name" VARCHAR(255),         
    "Initial release date" DATE,
    "Game type ID" TINYINT,                  
    "Total rating" NUMBER(5,2),        
    "Total rating count" INT,        
    "Hypes" SMALLINT 
);