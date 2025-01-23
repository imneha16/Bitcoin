from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

# Fetch Snowflake connection details
conn = BaseHook.get_connection("snowflake_conn")

# Define the DAG
with DAG(
    "BuildELT_BitcoinAnalysis",
    start_date=datetime(2024, 11, 23),
    description="A sample Airflow DAG to invoke dbt runs for Bitcoin analysis using a BashOperator",
    schedule=None,
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake",
        }
    },
) as dag:
    # dbt run task
    dbt_run_momentum_analysis = BashOperator(
        task_id="dbt_run_momentum_analysis",
        bash_command="/home/airflow/.local/bin/dbt run --models momentum_analysis --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
        dag=dag,
    )

    # Run the trend analysis model
    dbt_run_trend_analysis = BashOperator(
        task_id="dbt_run_trend_analysis",
        bash_command="/home/airflow/.local/bin/dbt run --models trend_analysis --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
        dag=dag,
    )

    # Run the volatility analysis model
    dbt_run_volatility_analysis = BashOperator(
        task_id="dbt_run_volatility_analysis",
        bash_command="/home/airflow/.local/bin/dbt run --models volatility_analysis --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
        dag=dag,
    )

    # Run the volume analysis model
    dbt_run_volume_analysis = BashOperator(
        task_id="dbt_run_volume_analysis",
        bash_command="/home/airflow/.local/bin/dbt run --models volume_analysis --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
        dag=dag,
    )

    # Run dbt tests for all models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="/home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
        dag=dag,
    )

    # Run dbt snapshot
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="/home/airflow/.local/bin/dbt snapshot --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
        dag=dag,
    )

    # Set dependencies for task execution
    (
        dbt_run_momentum_analysis
        >> dbt_run_trend_analysis
        >> dbt_run_volatility_analysis
        >> dbt_run_volume_analysis
        >> dbt_test
        >> dbt_snapshot
    )
