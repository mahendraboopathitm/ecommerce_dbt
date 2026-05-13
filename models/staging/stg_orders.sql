
-- models/staging/stg_orders.sql
{{
  config(
    materialized = 'view',
    description  = 'Cleaned order records with status normalization'
  )
}}

with source as (
    select * from {{ source('bronze', 'orders_raw') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date)                    as order_date,
        lower(order_status)                         as order_status,
        lower(payment_method)                       as payment_method,
        cast(order_total     as decimal(12,2))      as order_total,
        cast(shipping_cost   as decimal(8,2))       as shipping_cost,
        cast(shipped_at   as timestamp)             as shipped_at,
        cast(delivered_at as timestamp)             as delivered_at,
        promo_code,
        -- derived flags
        case when order_status = 'delivered'  then true else false end as is_delivered,
        case when order_status = 'cancelled'  then true else false end as is_cancelled,
        case when order_status = 'returned'   then true else false end as is_returned,
        case when promo_code  is not null      then true else false end as has_promo,
        cast(_loaded_at as timestamp)               as _loaded_at
    from source
    where order_id   is not null
      and customer_id is not null
)

select * from cleaned