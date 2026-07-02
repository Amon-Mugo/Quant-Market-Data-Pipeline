with source as (
    select * from {{ source('quant_pipeline_raw', 'ohlcv_raw') }}
),

deduped as (
    select
        symbol,
        trading_date,
        open,
        high,
        low,
        close,
        volume,
        sector

    from source
    qualify row_number() over (
        partition by symbol, trading_date
        order by trading_date, open, high, low, close, volume
    ) = 1
),

surrogate_keyed as (
    select
        concat(symbol, '-', cast(trading_date as string)) as ohlcv_id,
        *
    from deduped
)

select * from surrogate_keyed