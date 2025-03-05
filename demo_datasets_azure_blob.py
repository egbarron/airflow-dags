from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


def list_blobs_in_container(**kwargs):
    # Instantiate the hook using your connection ID
    wasb_hook = WasbHook(wasb_conn_id="class_datasets_azure_blob")
    # List blobs in a specified container
    blobs = wasb_hook.get_blobs_list(container_name="airflow")
    print("Blobs in container:", blobs)


with DAG(
    dag_id="demo_datasets_azure_blob",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    list_blobs_task = PythonOperator(
        task_id="list_blobs",
        python_callable=list_blobs_in_container,
        provide_context=True,
    )
