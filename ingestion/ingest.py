from ingestion.gcs_loader import upload_ndjson, upload_ticker_batch_ndjson
from ingestion.bq_loader import load_ticker_dates_to_bq, load_ticker_batch_to_bq
import pandas as pd
import argparse
import logging
import os
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "quant-pipeline-raw"
BACKFILL_YEARS = 5
INCREMENTAL_LOOKBACK_DAYS = 5
TICKER_DELAY_SECONDS = 10
MAX_RETRIES = 3
CONFIG_PATH = Path(__file__).parent.parent / "config" / "tickers.yaml"

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"


def load_tickers(config_path: Path = CONFIG_PATH) -> list[dict]:
    """
    Load the ticker universe from config/tickers.yaml.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        List of dicts like {"symbol": "AAPL", "sector": "technology"}.
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config["tickers"]


def fetch_ohlcv(symbol: str, start: date, end: date) -> list[dict]:
    """
    Fetch daily OHLCV rows for a single ticker over a date range
    using the Twelve Data API.
    """
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY not set. Check your .env file.")

    params = {
        "symbol": symbol,
        "interval": "1day",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "outputsize": 5000,
        "apikey": TWELVE_DATA_API_KEY,
    }

    response = requests.get(TWELVE_DATA_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if data.get("status") == "error":
        raise RuntimeError(f"Twelve Data error for {symbol}: {data.get('message')}")

    values = data.get("values", [])
    if not values:
        logger.warning(f"No data returned for {symbol} in range {start} to {end}.")
        return []

    rows = []
    for entry in values:
        trading_date = date.fromisoformat(entry["datetime"])

        if pd.isna(entry["open"]) or pd.isna(entry["close"]):
            logger.warning(
                f"Skipping {symbol} on {trading_date}: incomplete data (NaN)."
            )
            continue

        rows.append(
            {
                "symbol": symbol,
                "trading_date": trading_date,
                "open": round(float(entry["open"]), 4),
                "high": round(float(entry["high"]), 4),
                "low": round(float(entry["low"]), 4),
                "close": round(float(entry["close"]), 4),
                "volume": int(entry["volume"]),
            }
        )
    return rows


def fetch_ohlcv_with_retry(
    symbol: str, start: date, end: date, max_retries: int = MAX_RETRIES
) -> list[dict]:
    """
    Wrapper around fetch_ohlcv that retries with backoff if Twelve Data
    rate-limits or otherwise fails the request.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_ohlcv(symbol, start, end)
        except Exception as exc:
            if attempt == max_retries:
                logger.error(f"Giving up on {symbol} after {attempt} attempts: {exc}")
                return []
            wait = 10 * attempt
            logger.warning(
                f"Error fetching {symbol} (attempt {attempt}/{max_retries}): {exc}. "
                f"Retrying in {wait}s..."
            )
            time.sleep(wait)
    return []


def group_rows_by_date(rows: list[dict]) -> dict[date, list[dict]]:
    """
    Group a ticker's OHLCV rows by trading_date, since incremental GCS
    uploads happen one file per ticker per day.
    """
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["trading_date"]].append(row)
    return grouped


def run_ingestion(mode: str, bucket_name: str) -> None:
    tickers = load_tickers()
    today = date.today()

    if mode == "backfill":
        start = today - timedelta(days=365 * BACKFILL_YEARS)
    else:
        start = today - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    end = today + timedelta(days=1)  # keep end exclusive-style buffer

    logger.info(
        f"Starting {mode} ingestion for {len(tickers)} tickers, "
        f"range {start} to {today}"
    )

    for ticker in tickers:
        symbol = ticker["symbol"]
        sector = ticker["sector"]

        logger.info(f"Fetching {symbol} ({sector})...")
        rows = fetch_ohlcv_with_retry(symbol, start, end)

        for row in rows:
            row["sector"] = sector

        if mode == "backfill":
            all_dates = [str(row["trading_date"]) for row in rows]
            upload_ticker_batch_ndjson(bucket_name, symbol, rows)
            if all_dates:
                load_ticker_batch_to_bq(symbol, all_dates)
        else:
            grouped = group_rows_by_date(rows)
            loaded_dates = []
            for trading_date, day_rows in grouped.items():
                upload_ndjson(bucket_name, symbol, trading_date, day_rows)
                loaded_dates.append(str(trading_date))

            if loaded_dates:
                load_ticker_dates_to_bq(symbol, loaded_dates)

        time.sleep(TICKER_DELAY_SECONDS)  # respect Twelve Data rate limits

    logger.info("Ingestion complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV data into GCS.")
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental"],
        required=True,
        help="backfill: 5-year history. incremental: last 5 days (lookback).",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Target GCS bucket name (default: {DEFAULT_BUCKET}).",
    )
    args = parser.parse_args()

    run_ingestion(mode=args.mode, bucket_name=args.bucket)


if __name__ == "__main__":
    main()
