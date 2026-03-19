# This file orchestrates the ETL pipeline.

from medallion_arch.bronze.fetch_tickers import fetch_tickers
from medallion_arch.bronze.ingest_data_bronze import ingest_data_bronze
from medallion_arch.silver.clean_data_silver import clean_data_silver
from medallion_arch.gold.get_engine import get_engine
from medallion_arch.gold.load_into_db import load_into_db
from medallion_arch.gold.modeling_gold import modeling_gold
from etl_log_progress import log_progress

# Define bronze, silver, and gold output paths
bronze_path = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Financial Market Analysis/data_storage/bronze_layer.parquet"
silver_path = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Financial Market Analysis/data_storage/silver_layer.parquet"
gold_path = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Financial Market Analysis/data_storage/gold_layer.parquet"

# Define file path for logging of etl processes
log_file = "C:/Users/eolag/OneDrive/Documents/Data Engineering/Financial Market Analysis/data_storage/etl_log.txt"

def main():
    # Fetch tickers to be used in ETL process
    log_progress(log_file, "Beginning ETL process")
    tickers = fetch_tickers()
    log_progress(log_file, "Tickers Fetched")

    # Execute bronze layer
    ingest_data_bronze(tickers, bronze_path)
    log_progress(log_file, "Bronze Layer Processing Complete")

    # Execute silver layer
    clean_data_silver(bronze_path, silver_path)
    log_progress(log_file, "Silver Layer Processing Complete")

    # Establish connection to database
    engine = get_engine()
    log_progress(log_file, "Connection to Database Established")

    # Load cleaned silver data into Postgres database
    load_into_db(engine, silver_path)
    log_progress(log_file, "Silver Layer Data Loaded to Database")

    # Execute gold layer of medallion architecture
    modeling_gold(engine, gold_path)
    log_progress(log_file, "Gold Layer Processing Complete. ETL Complete") 

    
if __name__ == "__main__":
    main() 