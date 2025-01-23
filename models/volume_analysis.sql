WITH bitcoin_data AS (
  SELECT * FROM {{ source('bitcoin_data_db', 'bitcoin_prices') }}
),
volume_analysis AS (
  SELECT
    date,
    -- Ensure volume is not NULL; substitute 0 if it is NULL
    COALESCE(volume, 0) AS volume,
    -- Calculate 30-day rolling average, max, and min only when there are sufficient rows
    CASE
      WHEN COUNT(volume) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
        AVG(volume) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ELSE 0
    END AS avg_volume_30,
    CASE
      WHEN COUNT(volume) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
        MAX(volume) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ELSE 0
    END AS max_volume_30,
    CASE
      WHEN COUNT(volume) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
        MIN(volume) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
      ELSE 0
    END AS min_volume_30
  FROM bitcoin_data
  WHERE volume IS NOT NULL -- Ensure we only process non-NULL values for volume
)
SELECT * FROM volume_analysis
