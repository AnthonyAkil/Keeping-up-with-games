from airflow.sdk import dag, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


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
    def run_igdb_pipeline():
        
        import sys
        sys.path.append("/opt/airflow")
        
        try:
            from api_extract_data.api_to_storage import main as extract_load    # Pushes down the import as this is considered top-level code

            logger.info("Extracting and loading IGDB data...")
            extract_load()

        except Exception as e:
            logger.exception("IGDB pipeline failed")
            raise e

    run_igdb_pipeline()


igdb_pipeline()