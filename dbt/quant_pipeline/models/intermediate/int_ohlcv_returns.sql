with staged as (
    select * from {{ ref('stg_ohlcv') }}
),

returns as (
    select
        *,
        (close - lag(close) over (partition by symbol order by trading_date))
            / lag(close) over (partition by symbol order by trading_date) as daily_return,
        ln(close / lag(close) over (partition by symbol order by trading_date)) as log_return

    from staged
),

sequenced as (
    select
        *,
        row_number() over (partition by symbol order by trading_date) as ticker_row_num

    from returns
)

select * from sequenced