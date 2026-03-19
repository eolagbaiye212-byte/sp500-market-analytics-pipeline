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
Data Engineering/
├── Airflow/
│   ├── docker-compose.yaml          # Full Airflow cluster setup
│   ├── Dockerfile                   # Custom image with dependencies
│   ├── dags/
│   │   └── market_pipeline.py       # Airflow DAG definition
│   ├── config/
│   │   └── airflow.cfg              # Airflow configuration
│   ├── logs/                        # Execution logs
│   └── plugins/                     # Custom plugins
├── Financial Market Analysis/
│   ├── main.py                      # Standalone execution script
│   ├── etl_log_progress.py          # Logging utility
│   ├── validation_queries.sql       # SQL validation queries
│   └── data_storage/
│   └── medallion_arch/
│       ├── bronze/
│       │   ├── fetch_tickers.py         # Fetch S&P 500 tickers
│       │   └── ingest_data_bronze.py    # Bronze layer ingestion
│       ├── silver/
│       │   └── clean_data_silver.py     # Silver layer transformation
│       └── gold/
│       ├── get_engine.py            # Database connection
│       ├── load_into_db.py          # Data loading
│       └── modeling_gold.py         # Analytics modeling
└── README.md
```

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
   git clone <repository-url>
   cd "Data Engineering/Financial Market Analysis"
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install pandas yfinance sqlalchemy psycopg2-binary python-dotenv
   ```

4. **Set up PostgreSQL database**
   - Create a PostgreSQL database
   - Set environment variables (see Environment Variables section)

### Option 2: Airflow Mode

1. **Navigate to Airflow directory**
   ```bash
   cd "Data Engineering/Airflow"
   ```

2. **Start the Airflow cluster**
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow UI**
   - Open http://localhost:8080
   - Default credentials: admin/admin

## Environment Variables

Set the following environment variables for database connectivity:

| Variable | Description | Example |
|----------|-------------|---------|
| `PROJECT_DB_USER` | PostgreSQL username | `postgres` |
| `PROJECT_DB_PASSWORD` | PostgreSQL password | `mypassword` |
| `PROJECT_DB_HOST` | Database host | `localhost` |
| `PROJECT_DB_PORT` | Database port | `5432` |
| `PROJECT_DB_NAME` | Database name | `market_analytics` |

Create a `.env` file in the project root:

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
2. In the Airflow UI, enable the `market_pipeline` DAG
3. Trigger the DAG manually or wait for scheduled execution

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

**Eolagbaiye Olagbaiye**  
GitHub: [github.com/eolagbaiye212-byte](https://github.com/eolagbaiye212-byte)

---

*This project showcases modern data engineering practices for financial market analysis.*
