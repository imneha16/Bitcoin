WITH bitcoin_data AS (
  SELECT * FROM {{ source('bitcoin_data_db', 'bitcoin_prices') }}
),
price_changes AS (
  SELECT
    date,
    CLOSE,
    LAG(CLOSE) OVER (ORDER BY date) AS prev_close,
    CASE
      WHEN LAG(CLOSE) OVER (ORDER BY date) IS NOT NULL THEN CLOSE - LAG(CLOSE) OVER (ORDER BY date)
      ELSE 0
    END AS price_change,
    CASE
      WHEN LAG(CLOSE) OVER (ORDER BY date) IS NOT NULL AND LAG(CLOSE) OVER (ORDER BY date) != 0 
        THEN (CLOSE - LAG(CLOSE) OVER (ORDER BY date)) / LAG(CLOSE) OVER (ORDER BY date) * 100
      ELSE 0
    END AS price_change_percent
  FROM bitcoin_data
),
trend_analysis AS (
  SELECT
    date,
    CLOSE,
    price_change,
    price_change_percent,
    CASE
      WHEN price_change > 0 THEN 'Uptrend'
      WHEN price_change < 0 THEN 'Downtrend'
      ELSE 'No Change'
    END AS trend
  FROM price_changes
)
SELECT * FROM trend_analysis
