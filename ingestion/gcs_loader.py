import logging
import time
from datetime import date
from google.cloud import storage

logger = logging.getLogger(__name__)

MAX_UPLOAD_RETRIES = 3
UPLOAD_RETRY_BASE_WAIT_SECONDS = 10


def build_blob_path(symbol: str, as_of_date: date) -> str:
    return f"ticker={symbol}/dt={as_of_date.isoformat()}.json"


def records_to_ndjson(records: list[dict]) -> str:
    import json
    lines = (json.dumps(record, default=str) for record in records)
    return ("\n".join(lines) + "\n").encode("utf-8")


def upload_ndjson(
    bucket_name: str,
    symbol: str,
    as_of_date: date,
    records: list[dict],
    skip_if_exists: bool = True,
) -> str:
    if not records:
        logger.warning("No records to upload for %s on %s - skipping.", symbol, as_of_date)
        return

    blob_path = build_blob_path(symbol, as_of_date)
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)

            if skip_if_exists and blob.exists():
                logger.info("Already exists, skipping upload: %s", gcs_uri)
                return gcs_uri

            payload = records_to_ndjson(records)
            blob.upload_from_string(payload, content_type="application/json")
            logger.info("Uploaded %d records for %s to %s", len(records), symbol, gcs_uri)
            return gcs_uri
        except Exception as exc:
            if attempt == MAX_UPLOAD_RETRIES:
                logger.error(
                    "Giving up uploading %s on %s after %d attempts: %s",
                    symbol, as_of_date, attempt, exc,
                )
                raise
            wait = UPLOAD_RETRY_BASE_WAIT_SECONDS * attempt
            logger.warning(
                "Error uploading %s on %s (attempt %d/%d): %s. Retrying in %ds...",
                symbol, as_of_date, attempt, MAX_UPLOAD_RETRIES, exc, wait,
            )
            time.sleep(wait)

def build_batch_blob_path(symbol: str) -> str:
    return f"ticker={symbol}/full_history.json"


def upload_ticker_batch_ndjson(
    bucket_name: str,
    symbol: str,
    records: list[dict],
) -> str:
    """
    Uploads ALL of a ticker's records as a single NDJSON file, one write
    covering the full requested date range. Used by backfill mode, where
    writing one file per day for ~1,260 trading days is far too slow.
    Always overwrites - safe since GCS storage cost is negligible and this
    avoids any ambiguity about partial/stale batch files.
    """
    if not records:
        logger.warning("No records to upload for %s - skipping.", symbol)
        return

    blob_path = build_batch_blob_path(symbol)
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)

            payload = records_to_ndjson(records)
            blob.upload_from_string(payload, content_type="application/json")
            logger.info(
                "Uploaded %d records for %s to %s (batch)", len(records), symbol, gcs_uri
            )
            return gcs_uri
        except Exception as exc:
            if attempt == MAX_UPLOAD_RETRIES:
                logger.error(
                    "Giving up batch-uploading %s after %d attempts: %s",
                    symbol, attempt, exc,
                )
                raise
            wait = UPLOAD_RETRY_BASE_WAIT_SECONDS * attempt
            logger.warning(
                "Error batch-uploading %s (attempt %d/%d): %s. Retrying in %ds...",
                symbol, attempt, MAX_UPLOAD_RETRIES, exc, wait,
            )
            time.sleep(wait)
