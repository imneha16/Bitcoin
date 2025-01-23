WITH bitcoin_data AS (
  SELECT * FROM {{ source('bitcoin_data_db', 'bitcoin_prices') }}
),
momentum_calculations AS (
  SELECT
    date,
    CLOSE,
    LAG(CLOSE, 14) OVER (ORDER BY date) AS prev_close_14,  -- Calculate lag for 14 periods in SELECT clause
    LAG(CLOSE) OVER (ORDER BY date) AS prev_close,  -- Calculate previous CLOSE
    CLOSE - LAG(CLOSE, 14) OVER (ORDER BY date) AS momentum_14
  FROM bitcoin_data
),
price_changes AS (
  SELECT
    date,
    CLOSE,
    prev_close,
    momentum_14,
    SUM(CASE WHEN CLOSE > prev_close THEN 1 ELSE 0 END) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS positive_changes_14,
    SUM(CASE WHEN CLOSE < prev_close THEN 1 ELSE 0 END) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS negative_changes_14
  FROM momentum_calculations
),
rsi_calculations AS (
  SELECT
    date,
    CLOSE,
    momentum_14,
    positive_changes_14,
    negative_changes_14,
  CASE
    WHEN (positive_changes_14 + negative_changes_14) = 0 THEN 0  -- instead of NULL
    WHEN negative_changes_14 = 0 THEN 100
    ELSE ROUND(100 - (100 / (1 + (positive_changes_14 / negative_changes_14))), 2)
  END AS rsi_14
  FROM price_changes
)
SELECT * FROM rsi_calculations