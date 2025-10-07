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
import time
import polars as pl



# ============================================================================
# Script Configuration
# ============================================================================
api_rate_limit : int        = 500
rate_limit_delay : float    = 0.25     # 1 request / 0.25s
multiquery_size_limit :int  = 10


auth_url : str    = "https://id.twitch.tv/oauth2/token"
url      : str    = "https://api.igdb.com/v4/"

data_field_names : str = "id, name, first_release_date, game_modes, game_type, game_status, genres, platforms, total_rating, total_rating_count, dlcs, franchise, hypes"


parser = configparser.ConfigParser()
parser.read("credentials.conf")
client_id = parser.get(
    "igdb_credentials",
    "client_id"
)
client_secret = parser.get(
    "igdb_credentials",
    "client_secret"
)
auth_params = {
    "client_id":        client_id,
    "client_secret":    client_secret,
    "grant_type":       "client_credentials"
} 


local_output_file : str = "igdb_api_data_test.parquet"
  

# ============================================================================
# Calling api access token
# ============================================================================

response = requests.post(auth_url, params = auth_params)
access_token = response.json()["access_token"]
headers     = {
    "Client-ID":        client_id,
    "Authorization":    f"Bearer {access_token}",
    "Accept":           "application/json"
}


# ============================================================================
# Function setup
# ============================================================================


def get_total_games_count(base_url: str, input_headers : dict) -> int:
    
    response = requests.post(base_url + "games/count", headers = input_headers)
    response.raise_for_status()
    return response.json()['count']

def create_offset_batches(offset_list: list[int], step_size: int):
    """
    Create a list of lists, where each entry corresponds to the offset ranges required to execute 1 multiquery request.
    Yield allows for a single call and one-time calculation.
    """
    for start_point in range(0, len(offset_list), step_size):
        yield offset_list[start_point : start_point + step_size]


# ============================================================================
# Calling data endpoints
# ============================================================================

total_games_count   = get_total_games_count(base_url = url, input_headers= headers)
total_pages         = ceil(total_games_count / api_rate_limit)
offsets             = [page_number * api_rate_limit for page_number in range(total_pages)]
offset_batches      = list(create_offset_batches(offsets, multiquery_size_limit))
num_api_calls       = len(offset_batches)

dataframes = []

for batch_index, offset_batch in enumerate(offset_batches, start = 1):
    
    multiquery = ""
    for subquery_index, offset in enumerate(offset_batch):
        multiquery += f"""
        query games "page_{batch_index}_{subquery_index}" {{
        fields {data_field_names};
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
            batch_results.extend(data[subquery]["result"])
            
        if batch_results:
            dataframes.append(pl.DataFrame(batch_results))
        else:
            print(f"Empty batch on {batch_index}/{num_api_calls}")
        time.sleep(rate_limit_delay)
        
    except requests.exceptions.RequestException as e:
        print(f"Error on batch {batch_index}: {e}")
        print("Attempting to sleep 5 seconds before next call...")
        time.sleep(5)
        continue
        
 
merged_dataframe = pl.concat(dataframes, how = "vertical")
merged_dataframe.write_parquet(local_output_file, compression = "zstd", compression_level = 3) # zstd:3 is a good tradeoff between compression-read for analytical ELT
