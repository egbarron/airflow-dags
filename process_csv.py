import csv
import os
import tempfile

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DATA_URL = (
    "https://classdatasets.blob.core.windows.net/airflow/wine_quality.csv"
)
LOCAL_FILE = tempfile.NamedTemporaryFile(delete=False, delete_on_close=False)


def download_csv():
    response = requests.get(DATA_URL)
    with open(LOCAL_FILE.name, "wb") as csv_file:
        csv_file.write(response.content)


def print_csv_headers():
    with open(LOCAL_FILE.name, "r") as csv_file:
        reader = csv.reader(csv_file)
        print("    ".join(next(reader)))
        os.unlink(LOCAL_FILE.name)


with DAG(
    "process_csv",
) as dag:
    download_csv_task = PythonOperator(
        task_id="download_csv", python_callable=download_csv
    )

    print_csv_headers_task = PythonOperator(
        task_id="print_csv_headers", python_callable=print_csv_headers
    )

download_csv_task >> print_csv_headers_task
