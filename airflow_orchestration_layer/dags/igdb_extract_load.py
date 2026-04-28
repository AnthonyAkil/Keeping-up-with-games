from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

#HACK: Defining the mapping dictionary within the dag is not ideal and should be uplifted in the future
ENDPOINT_TABLE_MAP = {
    "games":                 "RAW_GAMES",
    "game_modes":            "RAW_GAMEMODE",
    "game_types":            "RAW_GAMETYPE",
    "genres":                "RAW_GENRE",
    "platforms":             "RAW_PLATFORM",
    "franchises":            "RAW_FRANCHISE",
    "game_statuses":         "RAW_GAMESTATUS",
    "popularity_types":      "RAW_POPTYPE",
    "popularity_primitives": "RAW_POPPRIMITIVE"
}

@dag(
    dag_id="igdb_pipeline",
    start_date=datetime(2026, 4, 18),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["igdb", "elt"],
)
def igdb_pipeline():

    @task(retries=2, retry_delay=300)
    def run_igdb_pipeline(run_date: int):
        
        
        
        try:
            from api_extract_data.api_to_storage import main as extract_load    # Pushes down the import as this is considered top-level code

            logger.info("Extracting and loading IGDB data...")
            extract_load(run_date)

        except Exception as e:
            logger.exception("IGDB pipeline failed")
            raise e

    @task(retries=2, retry_delay=300)
    def copy_into_snowflake(run_date: int):
        from api_extract_data.get_keyvault_secrets import get_secret_client, get_secret
        import os
        from dotenv import load_dotenv
        import snowflake.connector
        
        load_dotenv()

        
        client = get_secret_client(os.environ.get("AZURE_VAULT_URL"))

        conn = snowflake.connector.connect(
            account=get_secret(client, "snowflake-account"),
            user=get_secret(client, "snowflake-username"),
            password=get_secret(client, "snowflake-password"),
            database=get_secret(client, "snowflake-database"),
            schema=get_secret(client, "snowflake-schema"),
            warehouse=get_secret(client, "snowflake-warehouse"),
        )

        try:
            cursor = conn.cursor()
            for endpoint, table in ENDPOINT_TABLE_MAP.items():
                file_path = f"{run_date}/{endpoint}.parquet"
                sql = f"""
                    COPY INTO {table}
                    FROM @BLOB_PARQUET_STAGE
                    FILE_FORMAT = (TYPE = PARQUET)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = CONTINUE
                    FILES = ('{file_path}');
                """
                try:
                    logger.info(f"Copying {file_path} into {table}...")
                    cursor.execute(f"TRUNCATE TABLE {table}")
                    cursor.execute(sql)
                    logger.info(f"Successfully copied {endpoint} into {table}")
                except Exception as e:
                    logger.error(f"Failed COPY INTO for {endpoint}: {e}")
                    raise
        finally:
            cursor.close()
            conn.close()
            

    @task
    def get_run_date():
        context = get_current_context()
        return context["ds_nodash"]
    
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="dbt_transformations",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    run_date = get_run_date()

    run_igdb_pipeline(run_date) >> copy_into_snowflake(run_date) >> trigger_dbt


igdb_pipeline()