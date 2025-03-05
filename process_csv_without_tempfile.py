import csv
from io import StringIO

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DATA_URL = (
    "https://classdatasets.blob.core.windows.net/airflow/wine_quality.csv"
)


def download_csv():
    return requests.get(DATA_URL).text


def print_csv_headers(**context):
    ti = context["ti"]
    csv_text = ti.xcom_pull(task_ids="download_csv")
    reader = csv.reader(StringIO(csv_text))
    print("    ".join(next(reader)))


with DAG(
    "process_csv_without_tempfile",
) as dag:
    download_csv_task = PythonOperator(
        task_id="download_csv", python_callable=download_csv
    )

    print_csv_headers_task = PythonOperator(
        task_id="print_csv_headers", python_callable=print_csv_headers
    )

download_csv_task >> print_csv_headers_task
