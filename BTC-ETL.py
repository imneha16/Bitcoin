from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import requests


# Snowflake connection function
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


# Task to extract the last 200 days of Bitcoin data from CoinAPI
@task
def extract_bitcoin_data():
    api_key = Variable.get("coinapi_key")
    symbol = "BINANCE_SPOT_BTC_USDT"  # Adjusted to match the CoinAPI documentation
    url = f"https://rest.coinapi.io/v1/ohlcv/{symbol}/history?period_id=1DAY&limit=200"
    headers = {"X-CoinAPI-Key": api_key}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from CoinAPI: {response.text}")

    # Returning the JSON response containing the Bitcoin data
    bitcoin_data = response.json()
    return bitcoin_data


# Task to transform the extracted data into a format ready for loading into Snowflake
@task
def transform_bitcoin_data(bitcoin_data):
    results = []
    for record in bitcoin_data:
        transformed_record = {
            "date": record["time_period_start"],  # Start time of the OHLCV period
            "open": float(
                record.get("price_open", 0.0)
            ),  # Safeguard against missing keys
            "high": float(record.get("price_high", 0.0)),
            "low": float(record.get("price_low", 0.0)),
            "close": float(record.get("price_close", 0.0)),
            "volume": float(record.get("volume_traded", 0.0)),
        }
        results.append(transformed_record)
    return results


# Task to load the transformed data into Snowflake
@task
def load_to_snowflake(data):
    cur = return_snowflake_conn()
    cur.execute("CREATE DATABASE IF NOT EXISTS bitcoin_data_db;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS bitcoin_data_db.raw_data;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bitcoin_data_db.raw_data.bitcoin_prices (
            date TIMESTAMP_NTZ NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            PRIMARY KEY (date)
        );
        """
    )

    insert_query = """
    INSERT INTO bitcoin_data_db.raw_data.bitcoin_prices (date, open, high, low, close, volume)
    SELECT %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s
    WHERE NOT EXISTS (
        SELECT 1 FROM bitcoin_data_db.raw_data.bitcoin_prices
        WHERE date = %(date)s
    );
"""

    for record in data:
        cur.execute(insert_query, record)
    cur.close()


# Defining the DAG
with DAG(
    dag_id="bitcoin_data_ETL",
    start_date=datetime(2024, 11, 23),
    schedule_interval="0 3 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ETL", "CoinAPI"],
) as dag:

    raw_data = extract_bitcoin_data()
    transformed_data = transform_bitcoin_data(raw_data)
    load_to_snowflake(transformed_data)
