from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from utils.Python_scripts.Dimension_extraction import main


with DAG(
    "IGDB_dimension_extraction",
    start_date=datetime(2021, 1 ,1),
    catchup=False
) as dag:
    PythonOperator(
        task_id = "run_python",
        python_callable = main,
    )