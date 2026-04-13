USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS IGDB.SILVER
  COMMENT = 'Schema for tables where the data will be cleaned and structured.';