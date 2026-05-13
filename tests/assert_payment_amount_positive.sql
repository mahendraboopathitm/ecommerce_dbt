-- tests/assert_payment_amount_positive.sql
-- This test FAILS (returns rows) if any successful payment has a zero or negative amount

select
    payment_id,
    order_id,
    amount,
    payment_status
from {{ ref('stg_payments') }}
where is_successful = true
  and amount <= 0