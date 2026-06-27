import logging
from google.cloud import bigquery

logger=logging.getLogger(__name__)#logging.basicConfig(level=logging.INFO)

PROJECT_ID="gcp-de-learning-498109"
DATASET_ID="quant_pipeline_raw"
TABLE_ID="ohlcv_raw" #OHLCV_RAW
BUCKET_NAME="quant-pipeline-raw"


def load_gcs_uris_to_bq(gcs_uris: list[str]) -> int:# load_gcs_uris_to_bq
    if not gcs_uris:
        logger.warning("No URIs to load - skipping.")
        return 0
    client=bigquery.Client(project=PROJECT_ID) 
    table_ref=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config=bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    logger.info(f"Loading {len(gcs_uris)} file(s) into {table_ref}...")
    load_info=client.load_table_from_uri(
        gcs_uris, # Make this configurable
        table_ref, # Make this configurable
        job_config=job_config, # Make this configurable
    )

    load_job=load_info.result() # Wait for job to complete.
    destination_table=client.get_table(table_ref)
    rows_loaded=load_job.output_rows
    logger.info(
        f"Loaded {rows_loaded} row(s) into {table_ref}"
        f"(table now has {destination_table.num_rows} table rows)")
    return rows_loaded


def load_ticker_dates_to_bq(ticker: str, dates: list[str]) -> int: #build_gcs_uris for each ticker and load to BQ
    
    gcs_uris = [
        f"gs://{BUCKET_NAME}/ticker={ticker}/dt={dt}.json" for dt in dates
    ]
    return load_gcs_uris_to_bq(gcs_uris) # return number of rows loaded

def load_all_raw_data_to_bq() -> int:
    wildcard_uris=f"gs://{BUCKET_NAME}/ticker=*/dt=*.json"
    return load_gcs_uris_to_bq([wildcard_uris]) # return number of rows loaded

if __name__ == "__main__": # for testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",)
    load_all_raw_data_to_bq()