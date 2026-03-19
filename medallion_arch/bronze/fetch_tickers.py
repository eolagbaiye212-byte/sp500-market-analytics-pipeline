# This script fetches tickers from the Wikipedia S&P 500 ticker table

import pandas as pd
import requests
import random
import logging
from io import StringIO

logger = logging.getLogger(__name__)

def fetch_tickers():
    logger.info("Bronze Layer Processing Initiated")
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers)
    logger.info(f"Wikipedia response status: {response.status_code}")

    if response.status_code != 200: # Raises ValueError if Wikipedia returns bad response, Airflow stops pipeline, returns task as failed
        raise ValueError (f"Failed to fetch S&P 500 tickers - status code: {response.status_code}")

    sp500 = pd.read_html(StringIO(response.text))[0]
    tickers = sp500["Symbol"].tolist() # Turns "Symbol" column of wikiepdia table which contains S&P 500 companies into a list; "Symbol" contains the tickers for the company
    random_tickers = random.sample(tickers, k=10) # Random selection of 10 tickers from the list
    logger.info(f"Tickers fetched: {random_tickers}")
    return random_tickers