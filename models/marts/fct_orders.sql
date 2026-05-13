-- models/marts/fct_orders.sql
{{
  config(
    materialized       = 'incremental',
    incremental_strategy = 'merge',
    unique_key         = 'order_id',
    description        = 'Core fact table: one row per order',
    tags               = ['marts', 'finance']
  )
}}

with orders as (
    select * from {{ ref('int_orders_with_items') }}

    {% if is_incremental() %}
      -- only pick up orders modified/added since last run
      where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

final as (
    select
        -- keys
        order_id,
        customer_id,

        -- dates
        order_date,
        date_trunc('week',  order_date)  as order_week,
        date_trunc('month', order_date)  as order_month,
        date_trunc('year',  order_date)  as order_year,
        dayofweek(order_date)            as order_day_of_week,

        -- status
        order_status,
        payment_method,
        is_delivered,
        is_cancelled,
        is_returned,
        has_promo,
        promo_code,

        -- financials
        order_total,
        shipping_cost,
        items_subtotal,
        total_discount,
        paid_amount,
        round(order_total - paid_amount, 2) as revenue_gap,

        -- items
        total_line_items,
        total_units,
        avg_discount_pct,

        -- payment
        has_successful_payment,
        has_refund,

        -- logistics
        shipped_at,
        delivered_at,
        case
            when shipped_at is not null and order_date is not null
            then datediff(shipped_at, cast(order_date as timestamp))
        end as days_to_ship,
        case
            when delivered_at is not null and shipped_at is not null
            then datediff(delivered_at, shipped_at)
        end as days_to_deliver,

        -- metadata
        _loaded_at
    from orders
)

select * from final