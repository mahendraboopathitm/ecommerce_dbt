-- models/marts/dim_products.sql
{{
  config(
    materialized = 'table',
    description  = 'Product dimension with sales performance metrics'
  )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

sales_metrics as (
    select
        oi.product_id,
        count(distinct oi.order_id)         as total_orders,
        sum(oi.quantity)                    as units_sold,
        sum(oi.line_total)                  as total_revenue,
        avg(oi.unit_price)                  as avg_selling_price,
        avg(oi.discount_pct)                as avg_discount_pct,
        max(o.order_date)                   as last_sold_date
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
    where not o.is_cancelled
    group by oi.product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.unit_price,
        p.cost_price,
        p.margin_pct,
        p.sku,
        p.is_active,
        p.created_at,

        -- sales metrics
        coalesce(s.total_orders,   0)  as total_orders,
        coalesce(s.units_sold,     0)  as units_sold,
        coalesce(s.total_revenue,  0)  as total_revenue,
        s.avg_selling_price,
        s.avg_discount_pct,
        s.last_sold_date,

        -- categorization
        case
            when coalesce(s.units_sold, 0) = 0          then 'no_sales'
            when coalesce(s.total_revenue, 0) >= 10000  then 'top_seller'
            when coalesce(s.total_revenue, 0) >= 3000   then 'good_seller'
            else 'slow_mover'
        end as sales_tier,

        p._loaded_at
    from products p
    left join sales_metrics s on p.product_id = s.product_id
)

select * from final