-- tests/assert_order_total_matches_items.sql
-- Flags orders where dbt-calculated items_subtotal deviates >10% from source order_total

select
    order_id,
    order_total,
    items_subtotal,
    abs(order_total - items_subtotal) as delta,
    abs(order_total - items_subtotal) / nullif(order_total, 0) as pct_diff
from {{ ref('fct_orders') }}
where abs(order_total - items_subtotal) / nullif(order_total, 0) > 0.10
  and order_status not in ('cancelled', 'returned')