USE ROLE ACCOUNTADMIN;      ---> Need more permissions to create the storage integration
USE WAREHOUSE COMPUTE_WH;

CREATE STORAGE INTEGRATION IF NOT EXISTS IGDB_AZURE_BLOB
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<TENTANT ID>'  -- INPUT TENANT ID HERE
  STORAGE_ALLOWED_LOCATIONS = ('azure://<STORAGE ACCOUNT NAME>.blob.core.windows.net/<BLOB CONTAINER NAME>/raw/');     -- Replace with your storage account and container names


---> Retrieve the consent URL, navigate to it and accept the permission request:
DESC INTEGRATION IGDB_AZURE_BLOB ->> 
    SELECT "property_value" 
    FROM $1 
    WHERE "property" = 'AZURE_CONSENT_URL';

----> Within Azure: grant Snowflake Access to the Storage Account via IAM -> role assignments -> Storage Blob Data Reader assigned to the following id:
DESC INTEGRATION IGDB_AZURE_BLOB ->> 
    SELECT SPLIT_PART("property_value",'_', 1) 
    FROM $1 
    WHERE "property" = 'AZURE_MULTI_TENANT_APP_NAME';

---> Pull allowed integration url (as configured above):
DESC INTEGRATION IGDB_AZURE_BLOB ->> 
    SELECT "property_value" 
    FROM $1 
    WHERE "property" = 'STORAGE_ALLOWED_LOCATIONS';
    
---> Create external stage
CREATE STAGE IF NOT EXISTS IGDB.BRONZE.BLOB_PARQUET_STAGE
  STORAGE_INTEGRATION = IGDB_AZURE_BLOB
  URL = ('azure://<STORAGE ACCOUNT NAME>.blob.core.windows.net/<BLOB CONTAINER NAME>/raw/')    -- Replace with your storage account and container names
  FILE_FORMAT = (TYPE = 'parquet');