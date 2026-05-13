-- models/staging/stg_order_items.sql
{{
  config(
    materialized = 'view',
    description  = 'Cleaned order line items'
  )
}}

with source as (
    select * from {{ source('bronze', 'order_items_raw') }}
),

cleaned as (
    select
        order_item_id,
        order_id,
        product_id,
        cast(quantity    as int)            as quantity,
        cast(unit_price  as decimal(10,2))  as unit_price,
        cast(discount_pct as decimal(5,4))  as discount_pct,
        cast(line_total  as decimal(12,2))  as line_total,
        -- derived
        round(unit_price * quantity, 2)     as gross_amount,
        round(unit_price * quantity * discount_pct, 2) as discount_amount,
        cast(_loaded_at as timestamp)       as _loaded_at
    from source
    where order_item_id is not null
      and quantity >= 1
      and unit_price > 0
)

select * from cleaned