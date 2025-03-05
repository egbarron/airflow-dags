import datetime
import os

import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

PRICES_FILE = "/tmp/stock_prices.txt"
SYMBOLS_FILE = "/tmp/stocks.txt"
VOLUMES_FILE = "/tmp/stock_volumes.txt"


def read_stock_symbols():
    with open(SYMBOLS_FILE, "r") as f:
        return [line.strip() for line in f.readlines()]


def lookup_stock_prices(symbols: list[str]):
    prices = dict(
        zip(
            symbols,
            map(
                lambda s: yf.Ticker(s).history(period="1d")["Close"][0],
                symbols,
            ),
        )
    )
    with open(PRICES_FILE, "w") as f:
        for symbol, price in prices.items():
            f.write(f"{symbol}: {float(price):.02f}\n")


def lookup_stock_volumes(symbols: list[str]):
    volumes = dict(
        zip(
            symbols,
            map(
                lambda s: yf.Ticker(s).history(period="1d")["Volume"][0],
                symbols,
            ),
        )
    )
    with open(VOLUMES_FILE, "w") as f:
        for symbol, volume in volumes.items():
            f.write(f"{symbol}: {int(volume)}\n")


def delete_symbols_file():
    os.remove(SYMBOLS_FILE)


with DAG("lookup_stocks", start_date=datetime.datetime(2025,3,3), schedule=None, catchup=False) as dag:
    wait_for_symbols_file_task = FileSensor(
        task_id="wait_for_symbols_file",
        filepath=SYMBOLS_FILE,
        poke_interval=10,
    )

    read_stock_symbols_task = PythonOperator(
        task_id="read_stock_symbols", python_callable=read_stock_symbols
    )

    with TaskGroup(group_id="lookup_group") as lookup_group:
        lookup_stock_prices_task = PythonOperator(
            task_id="lookup_stock_prices",
            python_callable=lookup_stock_prices,
            op_args=[read_stock_symbols_task.output],
        )

        lookup_stock_volumes_task = PythonOperator(
            task_id="lookup_stock_volumes",
            python_callable=lookup_stock_volumes,
            op_args=[read_stock_symbols_task.output],
        )

    delete_symbols_file_task = PythonOperator(
        task_id="delete_symbols_file", python_callable=delete_symbols_file
    )

    wait_for_symbols_file_task >> read_stock_symbols_task >> lookup_group >> delete_symbols_file_task
