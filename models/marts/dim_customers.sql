-- models/marts/dim_customers.sql
{{
  config(
    materialized = 'table',
    description  = 'Customer dimension enriched with lifetime order metrics'
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_metrics as (
    select
        customer_id,
        count(*)                                    as total_orders,
        count(case when is_delivered then 1 end)    as delivered_orders,
        count(case when is_cancelled then 1 end)    as cancelled_orders,
        sum(order_total)                            as lifetime_revenue,
        avg(order_total)                            as avg_order_value,
        max(order_date)                             as last_order_date,
        min(order_date)                             as first_order_date,
        datediff(current_date(), max(order_date))   as days_since_last_order,
        count(case when has_promo then 1 end)       as promo_order_count
    from {{ ref('fct_orders') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        c.country,
        c.signup_date,
        c.is_active,
        c.customer_tier,

        -- order metrics
        coalesce(om.total_orders,       0)  as total_orders,
        coalesce(om.delivered_orders,   0)  as delivered_orders,
        coalesce(om.cancelled_orders,   0)  as cancelled_orders,
        coalesce(om.lifetime_revenue,   0)  as lifetime_revenue,
        coalesce(om.avg_order_value,    0)  as avg_order_value,
        om.last_order_date,
        om.first_order_date,
        om.days_since_last_order,
        coalesce(om.promo_order_count,  0)  as promo_order_count,

        -- segmentation
        case
            when om.lifetime_revenue >= 5000 then 'vip'
            when om.lifetime_revenue >= 1000 then 'high_value'
            when om.lifetime_revenue >= 200  then 'mid_value'
            when om.total_orders     >  0    then 'low_value'
            else 'no_purchase'
        end as value_segment,

        case
            when om.days_since_last_order <= 30  then 'active'
            when om.days_since_last_order <= 90  then 'at_risk'
            when om.days_since_last_order <= 180 then 'lapsed'
            else 'churned'
        end as churn_segment,

        -- metadata
        c._loaded_at
    from customers c
    left join order_metrics om on c.customer_id = om.customer_id
)

select * from final