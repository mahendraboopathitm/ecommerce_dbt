-- models/staging/stg_products.sql
{{
  config(
    materialized = 'view',
    description  = 'Cleaned product catalog'
  )
}}

with source as (
    select * from {{ source('bronze', 'products_raw') }}
),

cleaned as (
    select
        product_id,
        trim(product_name)                   as product_name,
        trim(category)                       as category,
        trim(subcategory)                    as subcategory,
        trim(brand)                          as brand,
        cast(unit_price  as decimal(10,2))   as unit_price,
        cast(cost_price  as decimal(10,2))   as cost_price,
        upper(trim(sku))                     as sku,
        cast(is_active as boolean)           as is_active,
        cast(created_at as timestamp)        as created_at,
        -- derived
        round((unit_price - cost_price) / unit_price * 100, 2) as margin_pct,
        cast(_loaded_at as timestamp)        as _loaded_at
    from source
    where product_id is not null
      and unit_price  > 0
)

select * from cleaned