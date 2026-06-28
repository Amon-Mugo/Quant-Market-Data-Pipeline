from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

default_args = {  # default args for the operator
    "owner":"amon",
    "retries": 1,
    "retry_delay":600,
}

with DAG(    # DAG declaration
    dag_id="quant_pipeline_ohlcv",  # DAG name
    description="Daily OHLCV ingestion (Twelve Data -> GCS -> BigQuery) + dbt run.",
    default_args=default_args,# default args
    start_date=datetime(2026,6,1), # start date
    schedule="@daily", # schedule
    catchup=False, # allow for catchup
    max_active_runs=1, # only one active run at a time
    params={
        "mode":Param(
            default="incremental",
            type="string",
            enum=["incremental","backfill"],
            description="incremental: last 5 days. backfill: full 5-year history (safe to rerun per-ticker).",
        ),
    },

    tags=["quant","pipeline"],
) as dag:
    ingest_and_load=BashOperator(
        task_id="ingest_and_load",  # task name
        bash_command=(
            "cd /opt/airflow/repo && "# change to repo directory
            "python -m ingestion.ingest --mode {{ params.mode }}" # run ingestion
        ),
    )

    dbt_run=BashOperator(
        task_id="dbt_run",
        bash_command='echo "dbt run placeholder — Week 3"',
    )

    ingest_and_load >> dbt_run