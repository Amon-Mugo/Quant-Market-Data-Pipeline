
import pandas as pd
import argparse
import logging
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from curl_cffi import requests
import yaml
import yfinance as yf

from ingestion.gcs_loader import upload_ndjson

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
    
   # Fetch daily OHLCV rows for a single ticker over a date range.

    
    session = requests.Session(impersonate="chrome")
    history = yf.Ticker(symbol, session=session).history(
        start=start, end=end, interval="1d"
    )

    rows = []
    for trading_date, row in history.iterrows():
        if pd.isna(row["Open"]) or pd.isna(row["Close"]):
            logger.warning(
                f"Skipping {symbol} on {trading_date.date()}: incomplete data (NaN)."
            )
            continue

        rows.append(
            {
                "symbol": symbol,
                "trading_date": trading_date.date(),
                "open": round(float(row["Open"]), 4),
                "high": round(float(row["High"]), 4),
                "low": round(float(row["Low"]), 4),
                "close": round(float(row["Close"]), 4),
                "volume": int(row["Volume"]),
            }
        )
    return rows


def fetch_ohlcv_with_retry(
    symbol: str, start: date, end: date, max_retries: int = MAX_RETRIES
) -> list[dict]:
    
    #Wrapper around fetch_ohlcv that retries with backoff if Yahoo Finance
   #rate-limits or otherwise fails the request.

    
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
    
    #Group a ticker's OHLCV rows by trading_date, since GCS uploads happen
    #one file per ticker per day.

  
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
    end = today + timedelta(days=1)  # yfinance end date is exclusive

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

        grouped = group_rows_by_date(rows)
        for trading_date, day_rows in grouped.items():
            upload_ndjson(bucket_name, symbol, trading_date, day_rows)

        time.sleep(TICKER_DELAY_SECONDS)  # be polite to Yahoo's unofficial API

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