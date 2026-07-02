from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

default_args = {  # default args for the operator
    "owner": "amon",
    "retries": 1,
    "retry_delay": 600,
}

DBT_PROJECT_DIR = "/opt/airflow/repo/dbt/quant_pipeline"
REPO_DIR = "/opt/airflow/repo"
DBT_LOG_PATH = "/tmp/dbt_logs"      # repo is read-only, dbt needs a writable log dir
DBT_TARGET_PATH = "/tmp/dbt_target"  # repo is read-only, dbt needs a writable target dir

with DAG(  # DAG declaration
    dag_id="quant_pipeline_ohlcv",  # DAG name
    description="Daily OHLCV ingestion (Twelve Data -> GCS -> BigQuery) + dbt transformation layer.",
    default_args=default_args,  # default args
    start_date=datetime(2026, 6, 1),  # start date
    schedule="@daily",  # schedule
    catchup=False,  # allow for catchup
    max_active_runs=1,  # only one active run at a time
    params={
        "mode": Param(
            default="incremental",
            type="string",
            enum=["incremental", "backfill"],
            description="incremental: last 5 days. backfill: full 5-year history (safe to rerun per-ticker).",
        ),
    },
    tags=["quant", "pipeline"],
) as dag:
    ingest_and_load = BashOperator(
        task_id="ingest_and_load",  # task name
        bash_command=(
            f"cd {REPO_DIR} && "  # change to repo directory
            "python -m ingestion.ingest --mode {{ params.mode }}"  # run ingestion
        ),
    )

    dbt_run_staging_intermediate = BashOperator(
        task_id="dbt_run_staging_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select path:models/staging path:models/intermediate "
            f"--log-path {DBT_LOG_PATH} --target-path {DBT_TARGET_PATH}"
        ),
    )

    compute_ema_rsi = BashOperator(
        task_id="compute_ema_rsi",
        bash_command=(
            f"cd {REPO_DIR} && "
            "python transform/compute_ema_rsi.py"
        ),
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select fct_ohlcv_indicators "
            f"--log-path {DBT_LOG_PATH} --target-path {DBT_TARGET_PATH}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--log-path {DBT_LOG_PATH} --target-path {DBT_TARGET_PATH}"
        ),
    )

    ingest_and_load >> dbt_run_staging_intermediate >> compute_ema_rsi >> dbt_run_mart >> dbt_test
