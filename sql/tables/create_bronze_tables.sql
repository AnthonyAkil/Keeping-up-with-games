USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

---> create the bronze table to land raw data
CREATE TABLE IF NOT EXISTS IGDB.BRONZE.RAW_GAMES (
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
CREATE TABLE IF NOT EXISTS IGDB.BRONZE.RAW_GAMEMODE (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255),
    created_at NUMBER(38,0),    -- storing UNIX timestamps
    slug VARCHAR(255),
    updated_at NUMBER(38,0),    -- storing UNIX timestamps
    url VARCHAR(255),
    checksum VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS IGDB.BRONZE.RAW_GENRE (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255),
    created_at NUMBER(38,0),    -- storing UNIX timestamps
    slug VARCHAR(255),
    updated_at NUMBER(38,0),    -- storing UNIX timestamps
    url VARCHAR(255),
    checksum VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS IGDB.BRONZE.RAW_PLATFORM (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255),
    created_at NUMBER(38,0),    -- storing UNIX timestamps
    slug VARCHAR(255),
    updated_at NUMBER(38,0),    -- storing UNIX timestamps
    url VARCHAR(255),
    checksum VARCHAR(255),
    alternative_name VARCHAR(255),
    generation SMALLINT,
    platform_logo SMALLINT,
    versions VARIANT, 
    platform_type SMALLINT,
    platform_family SMALLINT,
    abbreviation VARCHAR(255),
    summary TEXT                -- Stores game summary
);
  
CREATE TABLE IF NOT EXISTS IGDB.BRONZE.RAW_FRANCHISE (
    id SMALLINT PRIMARY KEY,               
    name VARCHAR(255),
    created_at NUMBER(38,0),    -- storing UNIX timestamps
    slug VARCHAR(255),
    updated_at NUMBER(38,0),    -- storing UNIX timestamps
    url VARCHAR(255),
    checksum VARCHAR(255),
    games VARIANT
);

CREATE TABLE IF NOT EXISTS IGDB.BRONZE.RAW_GAMETYPE (
    id SMALLINT PRIMARY KEY,               
    type VARCHAR(255),
    created_at NUMBER(38,0),    -- storing UNIX timestamps
    updated_at NUMBER(38,0),    -- storing UNIX timestamps
    checksum VARCHAR(255)
    
);