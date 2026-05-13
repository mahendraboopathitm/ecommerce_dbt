-- models/intermediate/int_orders_with_items.sql
{{
  config(
    materialized = 'ephemeral',
    description  = 'Orders joined with aggregated item metrics'
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items_agg as (
    select
        order_id,
        count(*)                        as total_line_items,
        sum(quantity)                   as total_units,
        sum(line_total)                 as items_subtotal,
        sum(discount_amount)            as total_discount,
        avg(discount_pct)               as avg_discount_pct,
        max(unit_price)                 as max_item_price,
        min(unit_price)                 as min_item_price
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments_agg as (
    select
        order_id,
        max(case when is_successful then amount end) as paid_amount,
        bool_or(is_successful)                       as has_successful_payment,
        bool_or(is_refunded)                         as has_refund
    from {{ ref('stg_payments') }}
    group by order_id
),

final as (
    select
        o.*,
        coalesce(oi.total_line_items,  0)       as total_line_items,
        coalesce(oi.total_units,        0)       as total_units,
        coalesce(oi.items_subtotal,     0)       as items_subtotal,
        coalesce(oi.total_discount,     0)       as total_discount,
        oi.avg_discount_pct,
        oi.max_item_price,
        oi.min_item_price,
        coalesce(p.paid_amount, 0)               as paid_amount,
        coalesce(p.has_successful_payment, false) as has_successful_payment,
        coalesce(p.has_refund, false)            as has_refund
    from orders o
    left join order_items_agg oi on o.order_id = oi.order_id
    left join payments_agg     p on o.order_id = p.order_id
)

select * from final