
# Loads NDJSON files from the raw GCS bucket into the BigQuery raw table.
# Called automatically by ingest.py after a successful GCS upload run,
# but can also be invoked standalone to (re)load a date range or all data.

import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = "gcp-de-learning-498109"
DATASET_ID = "quant_pipeline_raw"
TABLE_ID = "ohlcv_raw"
BUCKET_NAME = "quant-pipeline-raw"


def get_existing_dates(ticker: str, dates: list[str]) -> set[str]:
    
    #Queries BigQuery to find which of the given dates already have a row
    #for this ticker, so we can skip re-loading them.

    
    if not dates:
        return set()

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    query = f"""
        SELECT DISTINCT CAST(trading_date AS STRING) AS trading_date
        FROM `{table_ref}`
        WHERE symbol = @symbol
        AND trading_date IN UNNEST(@dates)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", ticker),
            bigquery.ArrayQueryParameter("dates", "DATE", dates),
        ]
    )
    results = client.query(query, job_config=job_config).result()
    return {row.trading_date for row in results}


def load_gcs_uris_to_bq(gcs_uris: list[str]) -> int:
    
    #Loads one or more NDJSON files from GCS into the BigQuery raw table.

    
    if not gcs_uris:
        logger.warning("No GCS URIs provided to load_gcs_uris_to_bq; skipping.")
        return 0

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    logger.info(f"Loading {len(gcs_uris)} file(s) into {table_ref}...")
    load_job = client.load_table_from_uri(
        gcs_uris,
        table_ref,
        job_config=job_config,
    )
    load_job.result()  # Blocks until the load job completes

    destination_table = client.get_table(table_ref)
    rows_loaded = load_job.output_rows
    logger.info(
        f"Loaded {rows_loaded} row(s) into {table_ref} "
        f"(table now has {destination_table.num_rows} total rows)."
    )
    return rows_loaded


def load_ticker_dates_to_bq(ticker: str, dates: list[str]) -> int:
    """
    Builds GCS URIs for a single ticker across a list of dates and loads
    them into BigQuery, skipping any dates that are already present for
    that ticker to avoid duplicate rows.

    Args:
        ticker: Ticker symbol, e.g. "AAPL"
        dates: List of date strings in YYYY-MM-DD format

    Returns:
        Number of rows loaded.
    """
    existing_dates = get_existing_dates(ticker, dates)
    new_dates = [d for d in dates if d not in existing_dates]

    skipped_count = len(dates) - len(new_dates)
    if skipped_count:
        logger.info(
            f"Skipping {skipped_count} already-loaded date(s) for {ticker}."
        )

    if not new_dates:
        logger.info(f"No new dates to load for {ticker}.")
        return 0

    gcs_uris = [
        f"gs://{BUCKET_NAME}/ticker={ticker}/dt={dt}.json" for dt in new_dates
    ]
    return load_gcs_uris_to_bq(gcs_uris)


def load_all_raw_data_to_bq() -> int:
    """
    Loads all NDJSON files currently in the raw bucket into BigQuery,
    using a wildcard URI. Useful for a full backfill load or recovery.
    Does NOT dedupe — only use on an empty or freshly truncated table.

    Returns:
        Number of rows loaded.
    """
    wildcard_uri = f"gs://{BUCKET_NAME}/ticker=*/dt=*.json"
    return load_gcs_uris_to_bq([wildcard_uri])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    load_all_raw_data_to_bq()