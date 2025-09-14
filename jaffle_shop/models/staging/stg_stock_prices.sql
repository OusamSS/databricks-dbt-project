{{ config(
    materialized='table',
    cluster_by=['date', 'symbol']
) }}

with source as (

    select * from {{ source('ecom','raw_prices') }}

)

select
    cast(date as date) as date,
    symbol,
    open,
    close,
    low,
    high,
    volume
from source
