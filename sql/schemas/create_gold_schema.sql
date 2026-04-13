USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS IGDB.GOLD
  COMMENT = 'Schema for tables where the data can be consumed by end users.';