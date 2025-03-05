import csv
import datetime
from io import StringIO

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def download_csv(data_url):
    return requests.get(data_url).text


def print_csv_headers(**context):
    ti = context["ti"]
    csv_text = ti.xcom_pull(task_ids="download_csv")
    reader = csv.reader(StringIO(csv_text))
    print("    ".join(next(reader)))


with DAG(
    "process_csv_parameterize_url",
    start_date=datetime.datetime(2025, 3, 4),
    schedule=datetime.timedelta(days=1),
) as dag:
    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
        op_args=[
            "https://classdatasets.blob.core.windows.net/airflow/wine_quality.csv"
        ],
    )

    print_csv_headers_task = PythonOperator(
        task_id="print_csv_headers", python_callable=print_csv_headers
    )

download_csv_task >> print_csv_headers_task
