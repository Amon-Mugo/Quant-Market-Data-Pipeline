-- SMA-20, SMA-50, and 20-day rolling volatility (stddev of daily returns).
-- Unlike EMA, these are straightforward window-function calculations:
-- each value only depends on a fixed lookback of raw rows, not on a
-- chain of previously computed indicator values, so no recursion is
-- needed here.

with base as (
    select * from {{ ref('int_ohlcv_returns') }}
),

moving_averages as (
    select
        *,
        avg(close) over (
            partition by symbol order by trading_date
            rows between 19 preceding and current row
        ) as sma_20_raw,
        avg(close) over (
            partition by symbol order by trading_date
            rows between 49 preceding and current row
        ) as sma_50_raw,
        stddev(daily_return) over (
            partition by symbol order by trading_date
            rows between 19 preceding and current row
        ) as volatility_20d_raw

    from base
),


windowed as (
    select
        symbol,
        trading_date,
        ticker_row_num,
        case when ticker_row_num >= 20 then sma_20_raw else null end as sma_20,
        case when ticker_row_num >= 50 then sma_50_raw else null end as sma_50,
        case when ticker_row_num >= 20 then volatility_20d_raw else null end as volatility_20d

    from moving_averages
)

select * from windowed