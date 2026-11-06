# S&P 500 Market Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.8+-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1.8-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue)
![Docker](https://img.shields.io/badge/Docker-20.10+-blue)

## Project Overview

An end-to-end data pipeline that ingests S&P 500 stock data, processes it through a medallion architecture (bronze, silver, gold layers), and produces analytics-ready metrics stored in PostgreSQL. The pipeline can be run standalone via `main.py` or orchestrated automatically with Apache Airflow.

This project demonstrates modern data engineering practices including data ingestion, transformation, storage, and analytics modeling in a scalable, containerized environment.

## Pipeline Architecture

The pipeline follows a medallion architecture with three layers:

- **Bronze Layer**: Raw data ingestion
  - Fetches 10 random S&P 500 tickers from Wikipedia
  - Ingests raw OHLCV (Open, High, Low, Close, Volume) stock data via yfinance API
  - Stores data as Parquet files for efficient columnar storage

- **Silver Layer**: Data cleaning and transformation
  - Cleans raw data (handles missing values, data types)
  - Computes technical indicators: rolling averages, golden/death cross signals, daily returns
  - Stores transformed data as Parquet files

- **Gold Layer**: Analytics and modeling
  - Loads processed data into PostgreSQL database
  - Runs complex analytics queries using multi-CTE (Common Table Expression) SQL
  - Computes key metrics: YTD return, 30-day return, volatility, performance ranking using DENSE_RANK()

```
Raw Data (yfinance API)
        ↓
   Bronze Layer
   (Ingestion & Storage)
        ↓
   Silver Layer
   (Cleaning & Transformation)
        ↓
   Gold Layer
   (Analytics & PostgreSQL)
```

## Project Structure

```
sp500-market-analytics-pipeline/
├── dags/
│   └── market_pipeline.py           # Airflow DAG definition
├── config/
│   └── airflow.cfg                  # Airflow configuration
├── medallion_arch/
│   ├── bronze/
│   │   ├── fetch_tickers.py         # Fetch S&P 500 tickers
│   │   └── ingest_data_bronze.py    # Bronze layer ingestion
│   ├── silver/
│   │   └── clean_data_silver.py     # Silver layer transformation
│   └── gold/
│       ├── get_engine.py            # Database connection
│       ├── load_into_db.py          # Data loading
│       └── modeling_gold.py         # Analytics modeling
├── main.py                          # Standalone execution script
├── etl_log_progress.py              # Logging utility
├── validation_queries.sql           # SQL validation queries
├── Dockerfile                       # Custom Airflow image with project dependencies
├── docker-compose.yaml              # Full Airflow cluster setup
└── README.md
```

Airflow creates the `logs/` and `plugins/` folders when the cluster starts; they are not tracked in the repository.

## Prerequisites

- Python 3.8+
- Docker 20.10+
- Docker Compose
- PostgreSQL 13+ (for standalone mode)
- Git

## Setup and Installation

### Option 1: Standalone Mode

1. **Clone the repository**
   ```bash
   git clone https://github.com/eolagbaiye212-byte/sp500-market-analytics-pipeline.git
   cd sp500-market-analytics-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install pandas pyarrow yfinance requests sqlalchemy psycopg2-binary
   ```

4. **Set up PostgreSQL database**
   - Create a PostgreSQL database
   - Set environment variables (see Environment Variables section)

5. **Set output paths**
   - `main.py` defines the Bronze, Silver, and Gold Parquet paths and the log file path as absolute paths. Update `bronze_path`, `silver_path`, `gold_path`, and `log_file` to a folder on your machine (for example, a `data_storage/` folder inside the project).

### Option 2: Airflow Mode

1. **Clone the repository** (if you haven't already)
   ```bash
   git clone https://github.com/eolagbaiye212-byte/sp500-market-analytics-pipeline.git
   cd sp500-market-analytics-pipeline
   ```

2. **Create a `.env` file** in the project root with the database variables (see Environment Variables section). Docker Compose loads this file into every Airflow container.

3. **Point the project volume at your clone**
   - In `docker-compose.yaml`, the last entry under `volumes:` mounts the project code into `/opt/airflow/financial_market_analysis`, which is where the DAG imports `medallion_arch` from and writes its Parquet files. Replace the absolute path on the left side of that entry with the path to your clone, for example:
   ```yaml
   - .:/opt/airflow/financial_market_analysis
   ```

4. **Build and start the Airflow cluster**
   ```bash
   docker compose up -d --build
   ```

5. **Access Airflow UI**
   - Open http://localhost:8080
   - Default credentials: `airflow` / `airflow` (set `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in `.env` to change them)

## Environment Variables

Set the following environment variables for database connectivity:

| Variable | Description | Example |
|----------|-------------|---------|
| `PROJECT_DB_USER` | PostgreSQL username | `postgres` |
| `PROJECT_DB_PASSWORD` | PostgreSQL password | `mypassword` |
| `PROJECT_DB_HOST` | Database host | `localhost` |
| `PROJECT_DB_PORT` | Database port | `5432` |
| `PROJECT_DB_NAME` | Database name | `market_analytics` |

- **Airflow mode:** create a `.env` file in the project root; Docker Compose loads it automatically.
- **Standalone mode:** `main.py` does not read `.env`, so export these variables in your shell before running it.

Example `.env` file:

```env
PROJECT_DB_USER=postgres
PROJECT_DB_PASSWORD=mypassword
PROJECT_DB_HOST=localhost
PROJECT_DB_PORT=5432
PROJECT_DB_NAME=market_analytics
```

## How to Run

### Standalone Execution

1. Ensure environment variables are set
2. Run the main pipeline:
   ```bash
   python main.py
   ```

### Airflow Orchestration

1. Start the Airflow cluster (see Setup)
2. In the Airflow UI, enable the `market_pipeline` DAG (new DAGs start paused)
3. Trigger the DAG manually or wait for the scheduled run (6:00 PM Eastern, Monday through Friday)

## Example Output

The pipeline produces the following analytics metrics stored in PostgreSQL:

- **YTD Return**: Year-to-date percentage return for each ticker
- **30-Day Return**: Rolling 30-day percentage return
- **Volatility**: 30-day rolling volatility (standard deviation of returns)
- **Performance Rank**: DENSE_RANK() based on YTD return within the batch

Sample query result:
```sql
SELECT ticker, ytd_return, thirty_day_return, volatility, performance_rank
FROM analytics_metrics
ORDER BY performance_rank;
```

| ticker | ytd_return | thirty_day_return | volatility | performance_rank |
|--------|------------|-------------------|-----------|------------------|
| AAPL   | 15.23%    | 5.67%            | 0.024     | 1                |
| MSFT   | 12.45%    | 3.21%            | 0.019     | 2                |
| ...    | ...       | ...              | ...       | ...              |

## Technologies Used

- **Python**: Core programming language
- **pandas**: Data manipulation and analysis
- **yfinance**: Yahoo Finance API for stock data
- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Relational database for analytics storage
- **SQLAlchemy**: Python SQL toolkit
- **Parquet**: Columnar storage format
- **Docker**: Containerization platform

## Author

**Emmanuel Olagbaiye**  
GitHub: [github.com/eolagbaiye212-byte](https://github.com/eolagbaiye212-byte)

---

*This project showcases modern data engineering practices for financial market analysis.*
