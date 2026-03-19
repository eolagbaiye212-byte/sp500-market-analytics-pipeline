# This script executes the gold layer of the medallion architecture; Data is cleaned, aggregated, and ready for consumption by end-users.

import pandas as pd
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def modeling_gold(engine, gold_path):
    logger.info("Gold Layer Processing Started")

    # Query to create gold_layer data
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS finance_gold"))

    gold_layer_query = ("""CREATE TABLE IF NOT EXISTS finance_gold AS WITH ytd_agg AS (SELECT market_date, ticker, close, FIRST_VALUE(close) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close, LAST_VALUE(close) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close FROM finance_silver), ticker_ranked AS (SELECT DISTINCT(ticker), DENSE_RANK() OVER (ORDER BY ROUND(((last_close-first_close)/first_close) * 100, 2) DESC) AS ytd_performance_rank FROM ytd_agg) \
                         SELECT s.market_date, s.ticker, s.close, s.high, s.low, s.open, s.volume, s.ma_50, s.ma_200, s.daily_return, s.golden_cross, s.death_cross, ROUND(((s.close - LAG(s.close, 30) OVER (PARTITION BY s.ticker ORDER BY s.market_date))/(LAG(s.close, 30) OVER (PARTITION BY s.ticker ORDER BY s.market_date)))*100, 2) as return_30d, ROUND(((y.last_close - y.first_close)/y.first_close)*100, 2) AS ytd_return, CASE WHEN COUNT(s.daily_return) OVER (PARTITION BY s.ticker ORDER BY s.market_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 30 THEN ROUND(STDDEV(s.daily_return) OVER (PARTITION BY s.ticker ORDER BY s.market_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 4) ELSE NULL END AS volatility_30d, r.ytd_performance_rank FROM finance_silver s JOIN ytd_agg y ON s.ticker = y.ticker AND s.market_date = y.market_date JOIN ticker_ranked r ON s.ticker = r.ticker""")
    
    # Open connection and begin transaction to create gold layer
    with engine.begin() as conn:
        conn.execute(text(gold_layer_query))

    # Save gold table as parquet table
    query = "SELECT * FROM finance_gold"
    df = pd.read_sql(query, engine)
    df.to_parquet(gold_path)

    logger.info(f"Current number of records in gold layer: {len(df)}")
    logger.info("Golf Layer Processing Complete")