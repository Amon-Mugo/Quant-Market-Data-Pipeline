import logging
from datetime import date
from google.cloud import storage

logger=logging.getLogger(__name__)

def build_blob_path(symbol:str,as_of_date:date) -> str:
    return f"ticker={symbol}/dt={as_of_date.isoformat()}.json"
    

def  records_to_ndjson(records:list[dict]) -> str:
    import json
    lines = (json.dumps(record, default=str) for record in records)
    return ("\n".join(lines) + "\n").encode("utf-8")

def upload_ndjson(
        bucket_name:str,
        symbol:str,
        as_of_date:date,
        records : list[dict],) ->str:
    if not records:
        logger.warning("No records to upload for  %s on %s -skipping .", symbol,as_of_date)
        return
    client=storage.Client()
    bucket=client.bucket(bucket_name)
    blob_path = build_blob_path(symbol, as_of_date)
    blob=bucket.blob(blob_path)
    payload=records_to_ndjson(records)
    blob.upload_from_string(payload, content_type="application/json")
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Uploaded %d records for %s to %s", len(records), symbol, gcs_uri)
    return gcs_uri
