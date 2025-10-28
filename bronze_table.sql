---> set Role Context
USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> create the Database
CREATE OR REPLACE DATABASE keeping_up_with_games
  COMMENT = 'This database contains the objects used to serve analysis of the IGDB API.';

---> create the Schema
CREATE OR REPLACE SCHEMA keeping_up_with_games.s3_data
  COMMENT = 'Schema for tables loaded from S3';

---> create the bronze table to land raw data
CREATE OR REPLACE TABLE keeping_up_with_games.s3_data.bronze_raw
  (
 id SMALLINT PRIMARY KEY,               
 name VARCHAR(255),         
 first_release_date INT,
 game_modes ARRAY,          
 game_type TINYINT,        
 genres ARRAY,   
 platforms ARRAY,           
 total_rating NUMBER(5,2),        
 total_rating_count INT,    
 dlcs ARRAY,                
 franchise ARRAY,           
 hypes SMALLINT
  )
  COMMENT = 'Table to be loaded from S3 raw data';

---> create S3 storage integration
CREATE STORAGE INTEGRATION keeping_up_with_games_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::047218031706:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('*');

---> pull relevant information (ARN) for AWS IAM role:
DESC INTEGRATION keeping_up_with_games_S3;

---> not required, but creating a fileformat for reusability:
CREATE OR REPLACE FILE FORMAT S3_parquet
  TYPE = parquet;


  
---> create external stage
CREATE OR REPLACE STAGE keeping_up_with_games.s3_data.S3_stage
  STORAGE_INTEGRATION = keeping_up_with_games_S3
  URL = 's3://keeping-up-with-games2/'
  FILE_FORMAT = S3_parquet;

---> load using the latest file
COPY INTO keeping_up_with_games.s3_data.bronze_raw
  FROM @keeping_up_with_games.s3_data.S3_stage
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  PATTERN = '.*igdb_api_data_[0-9]{8}.*';

