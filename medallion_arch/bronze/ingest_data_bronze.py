# This script contains the Bronze layer of the medallion architecture; it ingests data from the yfinance API

import yfinance as yf
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def ingest_data_bronze(tickers, bronze_path):
    df = yf.download(tickers, period = "2y", auto_adjust = "True")
    # Downloads data from tickers specified in input over 2 year period
    # Data contained is close, high, low, open, volume for each ticker over given time period in multi-index column structure
    df.to_parquet(bronze_path)
    logger.info("Bronze Layer Processing Complete")
    return df