"""
Internet Games Data Base (IGDB) API Data Extraction script for weekly usage - Dimension tables
Extracts game data from the IGDB API and prepares files for DWH ingestion.
"""

# ============================================================================
# IMPORTS
# ============================================================================

from math import ceil
import polars as pl
from datetime import datetime
import time
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

# Connection IDs (configured in Airflow UI)
IGDB_HTTP_CONN_ID = "igdb_api"
IGDB_AUTH_CONN_ID = "igdb_auth"
AWS_CONN_ID = "aws_s3"

bucket_name = "keeping-up-with-games2" 

endpoints = ["game_types", "game_modes", "genres", "platforms", "franchises"]


# ============================================================================
# Function setup
# ============================================================================

def get_access_token():
    """Get OAuth2 access token from Twitch"""
    auth_hook = HttpHook(http_conn_id=IGDB_AUTH_CONN_ID, method='POST')
    
    # The client_id and client_secret should be in the connection extras
    conn = auth_hook.get_connection(IGDB_AUTH_CONN_ID)
    client_credentials = conn.extra_dejson
    
    params = {
        "client_id": client_credentials.get("client_id"),
        "client_secret": client_credentials.get("client_secret"),
        "grant_type": "client_credentials"
    }
    
    response = auth_hook.run(
        endpoint="oauth2/token",
        data=params
    )
    
    return response.json()["access_token"]

def get_item_count(http_hook: HttpHook, headers: dict, endpoint: str) -> int:
    """Get total count of items for an endpoint"""
    response = http_hook.run(
        endpoint=f"{endpoint}/count",
        headers=headers
    )
    return response.json()['count']

def create_offset_batches(offset_list: list[int], step_size: int):
    """
    Create a list of lists, where each entry corresponds to the offset ranges required to execute 1 multiquery request.
    Yield allows for a single call and one-time calculation.
    """
    for start_point in range(0, len(offset_list), step_size):
        yield offset_list[start_point : start_point + step_size]

def get_endpoint(http_hook: HttpHook, s3_hook: S3Hook, headers: dict, endpoint: str, field_names: str):
    """Extract data from IGDB API endpoint and upload to S3"""
    
    today = datetime.now().strftime('%Y%m%d')
    filename = f"{endpoint}_{today}.parquet"
    s3_key = filename
    
    item_count = get_item_count(http_hook, headers, endpoint=endpoint)
    
    if item_count <= 500:
        try:
            # Build IGDB query as plain text string
            query = f"fields {field_names}; limit {item_count};"
            
            response = http_hook.run(
                endpoint=endpoint,
                headers=headers,
                data=query  # Send as plain text string
            )
            data = response.json()
            
            # Check for empty data
            if not data:
                print(f"No data returned for {endpoint}")
                return None

            # Write to S3 using S3Hook
            df = pl.DataFrame(data)
            
            # Write parquet to bytes buffer
            buffer = BytesIO()
            df.write_parquet(buffer, compression="zstd", compression_level=3)
            buffer.seek(0)
            
            # Upload to S3
            s3_hook.load_bytes(
                bytes_data=buffer.getvalue(),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"{endpoint} uploaded to s3://{bucket_name}/{s3_key}")
            
        except Exception as e:
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
                response = http_hook.run(
                    endpoint="multiquery",
                    headers=headers,
                    data=multiquery
                )
                batch_results = []
                data = response.json()
                
                for subquery in range(len(data)):
                    batch_results.extend(data[subquery].get("result", []))
                    
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
        
        if dataframes:
            merged_dataframe = pl.concat(dataframes, how = "diagonal")

            # Write to buffer and upload to S3
            buffer = BytesIO()
            merged_dataframe.write_parquet(
                buffer,
                compression="zstd",
                compression_level=3
            )
            buffer.seek(0)
            
            s3_hook.load_bytes(
                bytes_data=buffer.getvalue(),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"{endpoint} uploaded to s3://{bucket_name}/{s3_key}")
        else:
            print(f"No data retrieved from the API for endpoint {endpoint}.")
             
    
# ============================================================================
# Data extraction and load to AWS S3 with main execution wrapper
# ============================================================================
    
def main():
    """Main execution function"""
    
    access_token = get_access_token()
    
    # Get client_id from auth connection
    auth_conn = HttpHook.get_connection(IGDB_AUTH_CONN_ID)
    client_id = auth_conn.extra_dejson.get("client_id")
    
    # Create headers dictionary
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # Create HTTP hook
    http_hook = HttpHook(
        http_conn_id = IGDB_HTTP_CONN_ID,
        method = 'POST'
    )
    
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id = AWS_CONN_ID)

    for endpoint in endpoints[1::]:
        get_endpoint(http_hook, s3_hook, headers, endpoint, "name")
        
    # HACK: Not DRY - but opting for quick deployment:
    for endpoint in endpoints[0:1]:
        get_endpoint(http_hook, s3_hook, headers, endpoint, "type")


if __name__ == "__main__":
    main()