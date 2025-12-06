from __future__ import annotations

import os
import json
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator

#config
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")

STOCK_SYMBOLS = ["IBM", "AAPL"]

POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "stockdb")
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432

#db connection helper
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
        )
        return conn
    except Exception as e:
        raise RuntimeError(f"Postgres connection failed: {e}")

#create table
def create_table():
    sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        price NUMERIC(18,4),
        volume BIGINT,
        open NUMERIC(18,4),
        high NUMERIC(18,4),
        low NUMERIC(18,4),
        previous_close NUMERIC(18,4),
        api_timestamp TIMESTAMP,
        inserted_at TIMESTAMP DEFAULT NOW()
    );
    """
    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()

#fetch data
def fetch_stock_data(**context):
    if not ALPHAVANTAGE_API_KEY:
        raise ValueError("ALPHAVANTAGE_API_KEY is missing")

    base_url = "https://www.alphavantage.co/query"
    all_data = {}

    for symbol in STOCK_SYMBOLS:
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": ALPHAVANTAGE_API_KEY,
        }

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"API request failed for {symbol}: {e}")
            continue

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"Invalid JSON for {symbol}")
            continue

        if "Global Quote" not in data or not data["Global Quote"]:
            print(f"Missing stock data for {symbol}")
            continue

        all_data[symbol] = data["Global Quote"]

    if not all_data:
        raise RuntimeError("No stock data fetched.")

    context["ti"].xcom_push(key="stock_data", value=all_data)

#parse and store data
def parse_and_store(**context):
    raw_data = context["ti"].xcom_pull(
        key="stock_data", task_ids="fetch_stock_data"
    )

    if not raw_data:
        raise RuntimeError("No data received from API task")

    def safe_get(d, key, cast=float):
        val = d.get(key)
        if not val:
            return None
        try:
            return cast(val)
        except:
            return None

    rows = []

    for symbol, quote in raw_data.items():
        price = safe_get(quote, "05. price")
        volume = safe_get(quote, "06. volume", int)
        open_ = safe_get(quote, "02. open")
        high = safe_get(quote, "03. high")
        low = safe_get(quote, "04. low")
        prev_close = safe_get(quote, "08. previous close")

        date_str = quote.get("07. latest trading day")
        api_time = None
        if date_str:
            try:
                api_time = datetime.strptime(date_str, "%Y-%m-%d")
            except:
                pass

        rows.append((
            symbol,
            price,
            volume,
            open_,
            high,
            low,
            prev_close,
            api_time
        ))

    insert_sql = """
        INSERT INTO stock_prices
        (symbol, price, volume, open, high, low, previous_close, api_timestamp)
        VALUES %s;
    """

    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
    finally:
        conn.close()

#dag definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Stock API → Postgres Pipeline",
) as dag:

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    fetch_task = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=fetch_stock_data,
        provide_context=True,
    )

    store_task = PythonOperator(
        task_id="parse_and_store",
        python_callable=parse_and_store,
        provide_context=True,
    )

    create_table_task >> fetch_task >> store_task
