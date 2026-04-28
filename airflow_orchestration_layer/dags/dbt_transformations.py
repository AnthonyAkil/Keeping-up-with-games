from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime


@dag(
    dag_id="dbt_transformations",
    start_date=datetime(2026, 4, 18),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["dbt", "transformations"],
)
def dbt_transformations():
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    az_tenant_id = os.environ.get("AZURE_TENANT_ID")
    az_client_id = os.environ.get("AZURE_CLIENT_ID")
    az_client_secret= os.environ.get("AZURE_CLIENT_SECRET")
    az_vault_url = os.environ.get("AZURE_VAULT_URL")
    
    common_env = {
        "AZURE_TENANT_ID":     az_tenant_id,
        "AZURE_CLIENT_ID":     az_client_id,
        "AZURE_CLIENT_SECRET": az_client_secret,
        "AZURE_VAULT_URL":     az_vault_url,
        "DBT_PRIVATE_KEY_PATH": "/tmp/rsa_key.p8",
    }
    

    # Why DockerOperator: spawns the dbt container per command
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-snowflake:latest",
        command="dbt run --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        environment= common_env
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="dbt-snowflake:latest",
        command="dbt test --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        environment=common_env
    )

    dbt_run >> dbt_test


dbt_transformations()