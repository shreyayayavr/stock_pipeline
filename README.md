# stock_pipeline

**Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)**

This project implements a fully automated, Dockerized data pipeline that:

Fetches live stock market data from the Alpha Vantage API

Parses and processes the JSON response

Stores structured stock data into a PostgreSQL database

Uses Apache Airflow for orchestration and scheduling

Runs completely using Docker Compose with a single command


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
   