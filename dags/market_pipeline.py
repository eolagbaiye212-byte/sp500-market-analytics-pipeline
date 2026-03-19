# This is the DAG script for the ticker pipeline that will be orchestrated by Airflow.

import pendulum
from airflow.sdk import dag, task
from medallion_arch.bronze.fetch_tickers import fetch_tickers
from medallion_arch.bronze.ingest_data_bronze import ingest_data_bronze
from medallion_arch.silver.clean_data_silver import clean_data_silver
from medallion_arch.gold.get_engine import get_engine
from medallion_arch.gold.load_into_db import load_into_db
from medallion_arch.gold.modeling_gold import modeling_gold

# Medallion Paths
bronze_path = "/opt/airflow/financial_market_analysis/data_storage/bronze_layer.parquet"
silver_path = "/opt/airflow/financial_market_analysis/data_storage/silver_layer.parquet"
gold_path = "/opt/airflow/financial_market_analysis/data_storage/gold_layer.parquet"

# Define DAG parameter
@dag(dag_id="market_pipeline",
      schedule = "0 18 * * 1-5", # Runs 6PM every weekday
      start_date = pendulum.datetime(2026, 3, 18, tz ="America/New_York"),
      catchup = False)

# Orchestrate pipeline
def market_pipeline():
    @task
    def run_fetch_tickers():
        return fetch_tickers()
    @task
    def run_bronze(random_tickers):
        ingest_data_bronze(random_tickers, bronze_path)
    @task
    def run_silver():
        clean_data_silver(bronze_path, silver_path)
    @task
    def run_gold():
        engine = get_engine() 
        load_into_db(engine, silver_path)
        modeling_gold(engine, gold_path)

    tickers = run_fetch_tickers()
    bronze_result = run_bronze(tickers)
    silver_result = run_silver()
    gold_result = run_gold()
    bronze_result >> silver_result >> gold_result

dag = market_pipeline()
