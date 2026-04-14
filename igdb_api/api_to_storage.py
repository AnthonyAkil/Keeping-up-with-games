"""
Internet Games Data Base (IGDB) API Data Extraction script
Extracts game data from the IGDB API and prepares files for DWH ingestion.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import requests
from math import ceil
import time
import polars as pl
import os
from dotenv import load_dotenv
from datetime import datetime
import logging


# ============================================================================
# Logging setup
# ============================================================================

logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================================
# Function setup
# ============================================================================

def get_item_count(base_url: str, input_headers : dict, endpoint : str) -> int:
    
    response = requests.post(base_url + endpoint + "/count", headers = input_headers)
    response.raise_for_status()
    return response.json()['count']

def create_offset_batches(offset_list: list[int], step_size: int):
    """
    Create a list of lists, where each entry corresponds to the offset ranges required to execute 1 multiquery request.
    Yield allows for a single call and one-time calculation.
    """
    for start_point in range(0, len(offset_list), step_size):
        yield offset_list[start_point : start_point + step_size]

def get_endpoint(base_url: str, input_headers : dict, endpoint : str, field_names: str, 
                 api_rate_limit: int, container_name: str, storage_options: dict,
                 multiquery_size_limit: int, rate_limit_delay: float, headers: dict):

    today = datetime.today().strftime('%Y%m%d')
    filename = f"{endpoint}.parquet"
    output_path = f"az://{container_name}/raw/{today}/{filename}"           # Ensuring every daily extract lands in it's own "folder"

    
    item_count = get_item_count(base_url, input_headers = input_headers, endpoint= endpoint)
    
    if item_count <= api_rate_limit:
        try:
            params = {
                "fields": field_names,       
                "limit": item_count
            }    
            response = requests.post(base_url + endpoint, headers=input_headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Check for empty data
            if not data:
                logging.info(f"No data returned for {endpoint}")
                return None

            # Write to Blob Storage
            df = pl.DataFrame(data)
            df.write_parquet(
                output_path,
                storage_options=storage_options,
                compression="zstd",
                compression_level=3
            )
            logging.info(f"{endpoint} uploaded to {output_path}")
            
        except requests.exceptions.RequestException as e:
            logging.info(f"API call failed for {endpoint}: {e}")
            raise
    
    else:
        total_pages         = ceil(item_count / api_rate_limit)
        offsets             = [page_number * api_rate_limit for page_number in range(total_pages)]
        offset_batches      = list(create_offset_batches(offsets, multiquery_size_limit))
        num_api_calls       = len(offset_batches)

        dataframes = []

        for batch_index, offset_batch in enumerate(offset_batches, start = 1):
            
            multiquery = ""
            for subquery_index, offset in enumerate(offset_batch):
                multiquery += f"""
                query {endpoint} "page_{batch_index}_{subquery_index}" {{
                fields {field_names};
                limit {api_rate_limit};
                offset {offset};
            }};
            """
            
            try:
                response = requests.post(base_url + "multiquery", headers = headers, data = multiquery)
                response.raise_for_status()
                batch_results = []
                data = response.json()
                
                for subquery in range(len(data)):
                    batch_results.extend(data[subquery].get("result", []))  # Ensures failed batch calls return an empty list
                    
                if batch_results:
                    df = pl.DataFrame(batch_results)
                    dataframes.append(df)
                    
                else:
                    logging.info(f"Empty batch on {batch_index}/{num_api_calls}")
                
                time.sleep(rate_limit_delay)
                
            except requests.exceptions.RequestException as e:
                logging.info(f"Error on batch {batch_index}: {e}")
                logging.info("Attempting to sleep 5 seconds before next call...")
                time.sleep(5)
                continue
        
        if dataframes:
            merged_dataframe = pl.concat(dataframes, how = "diagonal")      #HACK: using diagonal to let Polars handle missing columns or mismatch across data types when concat

    
            merged_dataframe.write_parquet(
                output_path,
                storage_options = storage_options,
                compression = "zstd",
                compression_level = 3   # zstd:3 -> tradeoff between compression-read for analytical ELT
            )
            logging.info(f"{endpoint} uploaded to {output_path}")
        else:
            logging.info(f"No data retrieved from the API for endpoint {endpoint}.")
         

def main():
    
    # ============================================================================
    # Script Configuration
    # ============================================================================

    load_dotenv()

    # API-related:
    api_rate_limit : int        = int(os.environ.get("API_RATE_LIMIT"))
    rate_limit_delay : float    = float(os.environ.get("API_RATE_LIMIT_DELAY"))
    multiquery_size_limit :int  = int(os.environ.get("MULTIQUERY_SIZE_LIMIT"))

    auth_url : str      = os.environ.get("AUTH_URL")
    base_url : str      = os.environ.get("BASE_URL")

    api_client_id       = os.environ.get("CLIENT_ID")
    api_client_secret   = os.environ.get("client_secret")

    auth_params = {
        "client_id":        api_client_id,
        "client_secret":    api_client_secret,
        "grant_type":       "client_credentials"
    } 

    # Azure-related:
    storage_account_name    = os.environ.get("ACCOUNT_NAME")
    container_name          = os.environ.get("CONTAINER_NAME")
    access_key              = os.environ.get("ACCESS_KEY")

    storage_options = {
        "account_name": storage_account_name,
        "account_key":  access_key,
    }

    
    
    # ============================================================================
    # Calling api access token
    # ============================================================================

    try:
        response = requests.post(auth_url, params = auth_params)
        access_token = response.json()["access_token"]
        headers     = {
        "Client-ID":        api_client_id,
        "Authorization":    f"Bearer {access_token}",
        "Accept":           "application/json"
        }
    except ConnectionError as e:
        logging.error(e)
       
    
    # ============================================================================
    # Data extraction and load to cloud storage
    # ============================================================================
        
    endpoints = ["games", "game_modes", "game_types", "genres", "platforms", "franchises"]
    data_fields = ["id, name, first_release_date, game_modes, game_type, genres, platforms, total_rating, total_rating_count, franchises, hypes, updated_at"] + ["*"] * (len(endpoints) - 1)

    for endpoint, fields in zip(endpoints, data_fields):
        get_endpoint(base_url, headers, endpoint, fields, api_rate_limit, container_name, storage_options, multiquery_size_limit, rate_limit_delay, headers)
        

if __name__ == "__main__":
    main()