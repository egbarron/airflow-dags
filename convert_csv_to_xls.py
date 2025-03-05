import functools
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID = Variable.get("aws_conn_id")
BUCKET_NAME = Variable.get("bucket_name")
LOCAL_DOWNLOAD_PATH = Path("/tmp/convert_csv_to_xls/")
NEW_SUBFOLDER = Variable.get("new_subfolder")


def get_object_keys(**context):
    hook = S3Hook(aws_conn_id="class_datasets_aws_bucket")
    keys = hook.list_keys(bucket_name="classdatasets")
    ti = context["ti"]
    ti.xcom_push(key="object_keys", value=keys)


def download_objects(**context):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    ti = context["ti"]
    keys = ti.xcom_pull(task_ids="get_object_keys", key="object_keys")
    LOCAL_DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
    csv_files = [key for key in keys if key.endswith(".csv")]
    download_csv = functools.partial(
        hook.download_file,
        bucket_name=BUCKET_NAME,
        local_path=str(LOCAL_DOWNLOAD_PATH),
    )
    csv_local_paths = [download_csv(csv_file) for csv_file in csv_files]
    ti.xcom_push(key="csv_local_paths", value=csv_local_paths)


def convert_objects(**context):
    xlsx_local_paths = []
    ti = context["ti"]
    csv_local_paths = ti.xcom_pull(
        task_ids="download_objects", key="csv_local_paths"
    )
    for csv_local_path in csv_local_paths:
        print(f"Processing {csv_local_path}...")
        xlsx_local_path = LOCAL_DOWNLOAD_PATH / Path(
            csv_local_path
        ).with_suffix(".xlsx")
        df = pd.read_csv(csv_local_path)
        df.to_excel(xlsx_local_path, index=False, engine="openpyxl")
        xlsx_local_paths.append(xlsx_local_path.name)
    ti.xcom_push(key="xlsx_local_paths", value=xlsx_local_paths)


def upload_objects(**context):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    ti = context["ti"]
    xlsx_local_paths = ti.xcom_pull(
        task_ids="convert_objects", key="xlsx_local_paths"
    )
    for xlsx_local_path in xlsx_local_paths:
        xlsx_key = NEW_SUBFOLDER + Path(xlsx_local_path).name
        hook.load_file(
            filename=LOCAL_DOWNLOAD_PATH / xlsx_local_path,
            key=xlsx_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
    shutil.rmtree(LOCAL_DOWNLOAD_PATH)


with DAG(
    dag_id="convert_csv_to_xls",
    start_date=datetime(2025, 1, 1),
) as dag:
    get_object_keys_task = PythonOperator(
        task_id="get_object_keys",
        python_callable=get_object_keys,
    )

    download_objects_task = PythonOperator(
        task_id="download_objects",
        python_callable=download_objects,
    )

    convert_objects_task = PythonOperator(
        task_id="convert_objects",
        python_callable=convert_objects,
    )

    upload_objects_task = PythonOperator(
        task_id="upload_objects",
        python_callable=upload_objects,
    )
    (
        get_object_keys_task
        >> download_objects_task
        >> convert_objects_task
        >> upload_objects_task
    )
