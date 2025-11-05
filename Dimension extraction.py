"""
Internet Games Data Base (IGDB) API Data Extraction script
Extracts game data from the IGDB API and prepares files for DWH ingestion.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import requests
import configparser
from math import ceil
import polars as pl
from datetime import datetime
import time




# ============================================================================
# Script Configuration
# ============================================================================
parser = configparser.ConfigParser()
parser.read("credentials.conf")

# API-related:
api_rate_limit : int        = 500
rate_limit_delay : float    = 0.25     # 1 request / 0.25s
multiquery_size_limit :int  = 10

auth_url : str    = "https://id.twitch.tv/oauth2/token"
url      : str    = "https://api.igdb.com/v4/"

api_client_id = parser.get(
    "igdb_credentials",
    "client_id"
)
api_client_secret = parser.get(
    "igdb_credentials",
    "client_secret"
)
auth_params = {
    "client_id":        api_client_id,
    "client_secret":    api_client_secret,
    "grant_type":       "client_credentials"
} 

# AWS-related:
aws_access_key = parser.get(
    "aws_credentials",
    "access_key"
)
aws_secret_key = parser.get(
    "aws_credentials",
    "secret_key"
)
bucket_name = parser.get(
    "aws_credentials",
    "bucket_name"
)

storage_options = {
    "aws_access_key_id":        aws_access_key,
    "aws_secret_access_key":    aws_secret_key,
    "aws_region":               "eu-north-1"
}


# ============================================================================
# Calling api access token
# ============================================================================

response = requests.post(auth_url, params = auth_params)
access_token = response.json()["access_token"]
headers     = {
    "Client-ID":        api_client_id,
    "Authorization":    f"Bearer {access_token}",
    "Accept":           "application/json"
}


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

def get_endpoint(base_url: str, input_headers : dict, endpoint : str, field_names: str):
    
    today = datetime.now().strftime('%Y%m%d')
    filename = f"{endpoint}_{today}.parquet"
    output_path = f"s3://{bucket_name}/{filename}"
    
    item_count = get_item_count(base_url, input_headers = input_headers, endpoint= endpoint)
    
    if item_count <= 500:
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
                print(f"No data returned for {endpoint}")
                return None

            # Write to S3
            df = pl.DataFrame(data)
            df.write_parquet(
                output_path,
                storage_options=storage_options,
                compression="zstd",
                compression_level=3
            )
            print(f"{endpoint} uploaded to {output_path}")
            
        except requests.exceptions.RequestException as e:
            print(f"API call failed for {endpoint}: {e}")
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
                response = requests.post(url + "multiquery", headers = headers, data = multiquery)
                response.raise_for_status()
                batch_results = []
                data = response.json()
                
                for subquery in range(len(data)):
                    batch_results.extend(data[subquery].get("result", []))  # Ensures failed batch calls return an empty list
                    
                if batch_results:
                    df = pl.DataFrame(batch_results)
                    dataframes.append(df)
                    
                else:
                    print(f"Empty batch on {batch_index}/{num_api_calls}")
                
                time.sleep(rate_limit_delay)
                
            except requests.exceptions.RequestException as e:
                print(f"Error on batch {batch_index}: {e}")
                print("Attempting to sleep 5 seconds before next call...")
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
            print(f"{endpoint} uploaded to {output_path}")
        else:
            print(f"No data retrieved from the API for endpoint {endpoint}.")
         
    
    
    
    
    
    
    
# ============================================================================
# Data extraction and load to AWS S3
# ============================================================================
    
endpoints = ["game_modes", "genres", "platforms", "franchises"]


for endpoint in endpoints:
    get_endpoint(url, headers, endpoint, "name")        # Pull only the name field for the dimension tables
    
