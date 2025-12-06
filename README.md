# stock_pipeline

**Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)**

This project implements a fully automated, Dockerized data pipeline that:

Fetches live stock market data from the Alpha Vantage API

Parses and processes the JSON response

Stores structured stock data into a PostgreSQL database

Uses Apache Airflow for orchestration and scheduling

Runs completely using Docker Compose with a single command

----

**Data Flow**

Alpha Vantage API

        ↓
   Airflow Scheduler

        ↓
   Python (requests)

        ↓
   JSON Parsing

        ↓
   PostgreSQL Database


-----

**.env configuration (excluded in gitignore)**

ALPHAVANTAGE_API_KEY = api_key_here

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=stockdb

AIRFLOW__CORE__FERNET_KEY=any_random_string_here
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False



   