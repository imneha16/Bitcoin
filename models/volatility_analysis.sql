WITH bitcoin_data AS (
  SELECT * FROM {{ source('bitcoin_data_db', 'bitcoin_prices') }}
),
volatility_calculations AS (
  SELECT
    date,
    CLOSE,
    CASE
      WHEN high IS NOT NULL AND low IS NOT NULL THEN (high - low)
      ELSE 0
    END AS daily_range,
    CASE
      WHEN COUNT(CLOSE) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
        STDDEV(CLOSE) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ELSE 0
    END AS stddev_30,
    CASE
      WHEN COUNT(CLOSE) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
        AVG(high - low) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ELSE 0
    END AS avg_daily_range_30
  FROM bitcoin_data
  WHERE high IS NOT NULL AND low IS NOT NULL -- Ensures high and low are not NULL
)
SELECT * FROM volatility_calculations
