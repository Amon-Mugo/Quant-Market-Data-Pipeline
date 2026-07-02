# transform/compute_ema_rsi.py
import logging

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = "gcp-de-learning-498109"
DATASET_ID = "quant_pipeline_raw"
SOURCE_TABLE_ID = "int_ohlcv_returns"
DESTINATION_TABLE_ID = "int_ema_rsi_python"

EMA_SHORT_WINDOW = 20
EMA_LONG_WINDOW = 50
RSI_WINDOW = 14


def fetch_ohlcv_returns() -> pd.DataFrame:
    """
    Reads the deduplicated, return-calculated OHLCV history from the
    dbt intermediate model. This is the source dbt's pure-SQL logic
    already prepared - this script only adds what SQL window functions
    can't express: EMA and Wilder-smoothed RSI, both of which are
    recursive over each ticker's full price history.
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE_ID}"

    query = f"""
        SELECT symbol, trading_date, ticker_row_num, close, daily_return
        FROM `{table_ref}`
        ORDER BY symbol, trading_date
    """
    logger.info(f"Reading source data from {table_ref}...")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} row(s) across {df['symbol'].nunique()} ticker(s).")
    return df


def compute_ema(group: pd.DataFrame, window: int) -> pd.Series:
    """
    True exponential moving average via pandas' ewm(), which implements
    the standard recursive formula natively: EMA_today = (close * multiplier)
    + (EMA_yesterday * (1 - multiplier)), with multiplier = 2 / (window + 1).
    adjust=False matches the textbook recursive definition rather than
    pandas' default weighted-average-of-all-history behaviour.
    """
    return group["close"].ewm(span=window, adjust=False).mean()


def compute_wilder_rsi(group: pd.DataFrame, window: int) -> pd.Series:
    """
    Wilder's original RSI smoothing, which is itself a recursive
    exponential average (same structural problem as EMA) with a
    smoothing factor of 1/window rather than 2/(window+1).
    """
    gain = group["daily_return"].clip(lower=0)
    loss = -group["daily_return"].clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    # When avg_loss is 0 (all gains, no losses in the window), RSI is
    # defined as 100 - division by zero would otherwise produce NaN/inf.
    rsi = rsi.where(avg_loss != 0, 100)
    return rsi


def apply_partial_window_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforces the project's "no partial windows" rule: indicator values
    are nulled out until each ticker has accumulated enough trading
    history for a full window, even though the recursive math itself
    starts producing numbers from day 1.
    """
    df.loc[df["ticker_row_num"] < EMA_SHORT_WINDOW, "ema_20"] = None
    df.loc[df["ticker_row_num"] < EMA_LONG_WINDOW, "ema_50"] = None
    df.loc[df["ticker_row_num"] < RSI_WINDOW, "rsi_14"] = None
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes EMA-20, EMA-50, and RSI-14 per ticker, processing each
    ticker's history independently so one symbol's recursion never
    bleeds into another's.
    """
    results = []
    for symbol, group in df.groupby("symbol", sort=False):
        group = group.sort_values("trading_date").reset_index(drop=True)
        group["ema_20"] = compute_ema(group, EMA_SHORT_WINDOW)
        group["ema_50"] = compute_ema(group, EMA_LONG_WINDOW)
        group["rsi_14"] = compute_wilder_rsi(group, RSI_WINDOW)
        results.append(group)

    combined = pd.concat(results, ignore_index=True)
    combined = apply_partial_window_nulls(combined)
    return combined[["symbol", "trading_date", "ema_20", "ema_50", "rsi_14"]]


def write_indicators_to_bq(df: pd.DataFrame) -> int:
    """
    Overwrites the destination table on every run (WRITE_TRUNCATE).
    EMA and Wilder RSI are recursive over a ticker's entire history,
    so there's no safe way to compute "just today's new value"
    incrementally - a full recompute is simpler, correct, and cheap
    at this data volume (~19k rows across 15 tickers).
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{DESTINATION_TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("trading_date", "DATE"),
            bigquery.SchemaField("ema_20", "FLOAT64"),
            bigquery.SchemaField("ema_50", "FLOAT64"),
            bigquery.SchemaField("rsi_14", "FLOAT64"),
        ],
    )

    logger.info(f"Writing {len(df)} row(s) to {table_ref} (WRITE_TRUNCATE)...")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    destination_table = client.get_table(table_ref)
    logger.info(f"Wrote {len(df)} row(s); table now has {destination_table.num_rows} total rows.")
    return len(df)


def main() -> int:
    source_df = fetch_ohlcv_returns()
    indicators_df = compute_indicators(source_df)
    return write_indicators_to_bq(indicators_df)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    main()