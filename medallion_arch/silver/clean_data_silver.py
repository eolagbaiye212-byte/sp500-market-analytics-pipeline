# This script contains the silver layer of the medallion architecture, where the cleaning and modeling of the data takes place

import pandas as pd
import logging

# Set options for dataframe display in terminal
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

logger = logging.getLogger(__name__)

def clean_data_silver(bronze_path, silver_path):
    logger.info("Silver Layer Processing Initiated")

    # Read bronze_path as df
    df = pd.read_parquet(bronze_path)

    # Flatten multi-index columns
    df.columns = ['_'.join(map(str,col)).strip() for col in df.columns]
    df = df.reset_index()
    print(df.head(10))

    # Use df_melt to change the format from wide to long
    df = df.melt(id_vars = "Date", var_name = "Metric_Ticker", value_name = 'Value')
    print(df.head(10), '\n')

    # Seperate Metric_Ticker into two seperate columns
    df[["Metric", "Ticker"]] = df["Metric_Ticker"].str.split("_", expand = True)
    print(df.head(10), '\n')

    # Use pivot_table to change data into one row per ticker per date for each metric
    df_clean = pd.pivot_table(df, index = ["Date", "Ticker"], columns = "Metric", values = "Value").reset_index()
    print(df_clean.head(10), '\n')

    # Create 50-day and 200-day moving averages
    df_clean = df_clean.sort_values(["Ticker", "Date"]).reset_index(drop=True) # Ensures data is sorted by date for each ticker prior to calculating moving averages
    df_clean["ma_50"] = df_clean.groupby("Ticker")["Close"].transform(lambda x: x.rolling(50).mean())
    df_clean["ma_200"] = df_clean.groupby("Ticker")["Close"].transform(lambda x: x.rolling(200).mean())

    # Create daily percentage return
    df_clean["Daily_Return"] = df_clean.groupby('Ticker')['Close'].pct_change() * 100

    # Create Golden Cross - bullish pattern where short-term moving average (typically 50-day) crosses above long-term moving average (typically 200-day)
    df_clean["Golden_Cross"] = (df_clean["ma_50"] > df_clean["ma_200"]) & (df_clean.groupby("Ticker")["ma_50"].shift(1) <= df_clean.groupby("Ticker")["ma_200"].shift(1))

    # Create Death Cross - bearish pattern where short-term moving average (typically 50-day) crosses below long-term moving average (typically 200-day)
    df_clean["Death_Cross"] = (df_clean["ma_50"] < df_clean["ma_200"]) & (df_clean.groupby("Ticker")["ma_50"].shift(1) >= df_clean.groupby("Ticker")["ma_200"].shift(1))

    # Round all columns to 2 decimal places; prices only have 2 decimal places
    df_clean = df_clean.round(2)
    print(df_clean.head(500), '\n')
    #print(df_clean.dtypes)

    # Reinforce datatypes
    df_clean["ma_50"] = df_clean["ma_50"].astype(float)
    df_clean["ma_200"] = df_clean["ma_200"].astype(float)
    df_clean["Daily_Return"] = df_clean["Daily_Return"].astype(float)

    df_clean["Date"] = df_clean["Date"].dt.date
    df_clean.rename(columns={"Date":"Market_Date"}, inplace = True) # Rename Date column to Market_Date to avoid using database keyword
    df_clean.columns = df_clean.columns.str.lower() # Make all columns in dataframe lowercase to avoid errors with Postgre case-folding 

    # Save silver data
    df_clean.to_parquet(silver_path)

    logger.info("Silver Layer Processing Complete")
    return df_clean