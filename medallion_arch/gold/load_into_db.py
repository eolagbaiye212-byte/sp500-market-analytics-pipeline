# This code loads the silver layer dataframe into the Postgre database as a table

# finance_silver table must be first defined in SQL before this script can run.
# Alternatively, a CREATE TABLE query may be used in function to create the table.
# CREATE TABLE IF NOT EXISTS finance_silver (market_date DATE, ticker VARCHAR(20), close DECIMAL(10,2), high DECIMAL(10,2), low DECIMAL(10,2), open DECIMAL(10,2), volume INT, ma_50 DECIMAL(10,2), ma_200 DECIMAL(10,2), daily_return DECIMAL(10,2), golden_cross BOOL, death_cross BOOL, CONSTRAINT uq_date_ticker UNIQUE (market_date, ticker));

import pandas as pd
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def load_into_db(engine, silver_path):
    df = pd.read_parquet(silver_path)

    # Load dataframe into a temporary table
    df.to_sql("temp_finance", engine, if_exists='replace', index = False)

    # Define SQL Upsert (PostgreSQL) query to insert new records and update existing records in the target table
    upsert_query = """INSERT INTO finance_silver (market_date, ticker, close, high, low, open, volume, ma_50, ma_200, daily_return, golden_cross, death_cross) SELECT market_date, ticker, close, high, low, open, volume, ma_50, ma_200, daily_return, golden_cross, death_cross FROM temp_finance ON CONFLICT (market_date, ticker) DO UPDATE SET close = EXCLUDED.close, high = EXCLUDED.high, low = EXCLUDED.low, open = EXCLUDED.open, volume = EXCLUDED.volume, ma_50 = EXCLUDED.ma_50, ma_200 = EXCLUDED.ma_200, daily_return = EXCLUDED.daily_return, golden_cross = EXCLUDED.golden_cross, death_cross = EXCLUDED.death_cross"""

    # Open the connection and begin the upsert transaction
    with engine.begin() as conn:
        conn.execute(text(upsert_query))

    logger.info("New Silver Data Loaded to Database")
    