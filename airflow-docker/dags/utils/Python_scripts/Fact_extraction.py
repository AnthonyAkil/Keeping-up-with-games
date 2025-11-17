"""
Internet Games Data Base (IGDB) API Data Extraction script for daily usage - Fact tables
Extracts game data from the IGDB API and prepares files for DWH ingestion.
"""

# ============================================================================
# IMPORTS
# ============================================================================

from math import ceil
import time
import polars as pl
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import BytesIO


# ============================================================================
# Script Configuration
# ============================================================================
# API-related:
api_rate_limit : int        = 500
rate_limit_delay : float    = 0.25     # 1 request / 0.25s
multiquery_size_limit :int  = 10

data_field_names : str = "id, name, first_release_date, game_modes, game_type, genres, platforms, total_rating, total_rating_count, franchises, hypes"

# Connection IDs (configured in Airflow UI)
IGDB_HTTP_CONN_ID = "igdb_api"
IGDB_AUTH_CONN_ID = "igdb_auth"
AWS_CONN_ID = "aws_s3"

bucket_name = "keeping-up-with-games2" 


# ============================================================================
# Function setup
# ============================================================================

def get_access_token():
    """Get OAuth2 access token from Twitch"""
    auth_hook = HttpHook(http_conn_id=IGDB_AUTH_CONN_ID, method='POST')
    
    # The client_id and client_secret should be in the connection extras
    conn = auth_hook.get_connection(IGDB_AUTH_CONN_ID)
    extra = conn.extra_dejson
    
    params = {
        "client_id": extra.get("client_id"),
        "client_secret": extra.get("client_secret"),
        "grant_type": "client_credentials"
    }
    
    response = auth_hook.run(
        endpoint="oauth2/token",
        data=params
    )
    
    return response.json()["access_token"]

def get_total_games_count(http_hook: HttpHook) -> int:
    """Get total count of games from IGDB API"""
    response = http_hook.run(
        endpoint="games/count"
    )
    return response.json()['count']

def create_offset_batches(offset_list: list[int], step_size: int):
    """
    Create a list of lists, where each entry corresponds to the offset ranges required to execute 1 multiquery request.
    Yield allows for a single call and one-time calculation.
    """
    for start_point in range(0, len(offset_list), step_size):
        yield offset_list[start_point : start_point + step_size]


# ============================================================================
# Data extraction with main execution wrapper
# ============================================================================

def main():
    """Main execution function"""
    
    # Initialize hooks
    access_token = get_access_token()
    
    # Get client_id from auth connection
    auth_conn = HttpHook.get_connection(IGDB_AUTH_CONN_ID)
    client_id = auth_conn.extra_dejson.get("client_id")
    
    # Create HTTP hook with authorization headers
    http_hook = HttpHook(
        http_conn_id=IGDB_HTTP_CONN_ID,
        method='POST'
    )
    
    # Add headers to the hook
    http_hook.headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # Prepare S3 output path
    filename = f"igdb_api_data_{datetime.today().strftime('%Y%m%d')}.parquet"
    s3_key = filename

    total_games_count   = get_total_games_count(http_hook=http_hook)
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
            response = http_hook.run(
                endpoint="multiquery",
                data=multiquery
            )
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
            
        except Exception as e:
            print(f"Error on batch {batch_index}: {e}")
            print("Attempting to sleep 5 seconds before next call...")
            time.sleep(5)
            continue
        
    # ============================================================================
    # Load to AWS S3
    # ============================================================================
    
    if dataframes:
        merged_dataframe = pl.concat(dataframes, how = "diagonal")      #HACK: using diagonal to let Polars handle missing columns or mismatch across data types when concat
        
        # Write to buffer
        buffer = BytesIO()
        merged_dataframe.write_parquet(
            buffer,
            compression="zstd",
            compression_level=3
        )
        buffer.seek(0)
        
        # Upload to S3
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Parquet file uploaded to s3://{bucket_name}/{s3_key}")
    else:
        print("No data retrieved from the API.")
          
if __name__ == "__main__":
    main()