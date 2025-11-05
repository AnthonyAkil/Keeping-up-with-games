"""
This SQL-script is used to create the database, tables and AWS S3 integration such that the data can be loaded in Snowflake.

"""
---> set Role Context
USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> create the Database
CREATE DATABASE IF NOT EXISTS IGDB
  COMMENT = 'This database contains the objects used to serve analysis of the IGDB API.';

USE DATABASE IGDB;

---> create the Schema
CREATE SCHEMA IF NOT EXISTS IGDB.BRONZE
  COMMENT = 'Schema for tables loaded from S3';

CREATE SCHEMA IF NOT EXISTS IGDB.GOLD
  COMMENT = 'Schema for production-ready tables ';

---> create S3 storage integration
CREATE STORAGE INTEGRATION IF NOT EXISTS IGDB_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::047218031706:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('*');

---> pull relevant information (ARN) for AWS IAM role:
DESC INTEGRATION IGDB_S3;

---> not required, but creating a fileformat for reusability:
CREATE FILE FORMAT IF NOT EXISTS S3_parquet
  TYPE = parquet;
  
---> create external stage
CREATE STAGE IF NOT EXISTS IGDB.BRONZE.S3_stage
  STORAGE_INTEGRATION = IGDB_S3
  URL = 's3://keeping-up-with-games2/'
  FILE_FORMAT = S3_parquet;




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
    franchise TINYINT,           
    hypes SMALLINT )
COMMENT = 'Table to be loaded from S3 raw data';


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



CREATE TABLE IF NOT EXISTS IGDB.GOLD.BRIDGE_PLATFORM (
    "Game ID" SMALLINT,
    "PLATFORM ID" TINYINT
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


---> create the Fact table to land each game
CREATE TABLE IF NOT EXISTS IGDB.GOLD.GAMES (
    "Game ID" SMALLINT PRIMARY KEY,               
    "Game name" VARCHAR(255),         
    "Initial release date" DATE,
    "Game type" TINYINT,                  
    "Total rating" NUMBER(5,2),        
    "Total rating count" INT,                  
    "Franchise ID" TINYINT,           
    "Hypes" SMALLINT 
);