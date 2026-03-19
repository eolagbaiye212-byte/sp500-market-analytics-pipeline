-- This code provides validation queries for the gold layer

-- Ensure close, high, low, open, volume have no missing values
SELECT COUNT(*) FROM finance_gold WHERE close IS NULL OR high IS NULL OR open IS NULL OR low IS NULL OR volume IS NULL;

-- Ensure columns that should have NULL have the appropiate amount of NULL values
-- ma_50: Should be 49
SELECT ticker, COUNT(*) FROM finance_gold WHERE ma_50 IS NULL GROUP BY ticker;
-- ma_200: Should be 199
SELECT ticker, COUNT(*) FROM finance_gold WHERE ma_200 IS NULL GROUP BY ticker;
-- return_30d: Should be 30
SELECT ticker, COUNT(*) FROM finance_gold WHERE return_30d IS NULL GROUP BY ticker;
-- volatility_30d: Should be 30
SELECT ticker, COUNT(*) FROM finance_gold WHERE volatility_30d IS NULL GROUP BY ticker;
-- daily_return: Should be 1
SELECT ticker, COUNT(*) FROM finance_gold WHERE daily_return IS NULL GROUP BY ticker;

-- Identify duplicates among groups tickers (market date is unique value)
SELECT ticker, COUNT(*) FROM finance_gold GROUP BY ticker;
SELECT ticker, COUNT(DUPLICATE(market_date)) as unique_dates FROM finance_gold GROUP BY ticker;

-- Confirm datatypes
SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'table_name' AND table_schema = 'table_schema' ORDER BY ordinal_position;