from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


@dag(start_date=datetime(2025, 3, 3), schedule="@daily", catchup=False)
def datasets_azure_blob_task_flow_exercise_04():
    @task
    def list_blobs_in_container(**kwargs):
        # Instantiate the hook using your connection ID
        wasb_hook = WasbHook(wasb_conn_id="class_datasets_azure_blob")
        # List blobs in a specified container
        blobs = wasb_hook.get_blobs_list(container_name="airflow")
        print("Blobs in container:", blobs)


dag_instance = datasets_azure_blob_task_flow_exercise_04()
