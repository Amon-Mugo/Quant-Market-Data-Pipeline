{{ config(materialized='table') }}

with returns as (

    select
        symbol,
        trading_date,
        sector,
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        log_return
    from {{ ref('int_ohlcv_returns') }}

),

sma_volatility as (

    select
        symbol,
        trading_date,
        sma_20,
        sma_50,
        volatility_20d
    from {{ ref('int_ohlcv_sma_volatility') }}

),

ema_rsi as (

    select
        symbol,
        trading_date,
        ema_20,
        ema_50,
        rsi_14
    from {{ source('quant_pipeline_raw', 'int_ema_rsi_python') }}

)

select
    returns.symbol,
    returns.trading_date,
    returns.sector,
    returns.open,
    returns.high,
    returns.low,
    returns.close,
    returns.volume,
    returns.daily_return,
    returns.log_return,
    sma_volatility.sma_20,
    sma_volatility.sma_50,
    sma_volatility.volatility_20d,
    ema_rsi.ema_20,
    ema_rsi.ema_50,
    ema_rsi.rsi_14
from returns
left join sma_volatility
    on returns.symbol = sma_volatility.symbol
    and returns.trading_date = sma_volatility.trading_date
left join ema_rsi
    on returns.symbol = ema_rsi.symbol
    and returns.trading_date = ema_rsi.trading_date
