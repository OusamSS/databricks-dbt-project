with source as (

    select * from {{ source('ecom','raw_prices') }}

)

select * from source
