
with daily_prices as (
    select
        date,
        symbol,
        close,
        volume,
        lag(close, 1) over (partition by symbol order by date) as prev_close
    from {{ ref('stg_stock_prices') }}
),

daily_metrics as (
    select
        date,
        symbol,
        close,
        volume,
        (close - prev_close) / prev_close as daily_return,
        ln(close / prev_close) as daily_log_return,
        avg(volume) over (
            partition by symbol
            order by date
            rows between 10 preceding and current row
        ) as volume_10d_avg,
        stddev(ln(close / prev_close)) over (
            partition by symbol
            order by date
            rows between 10 preceding and current row
        ) as daily_volatility_10d
    from daily_prices
)

select * from daily_metrics