-- models/staging/stg_payments.sql
{{
  config(
    materialized = 'view',
    description  = 'Cleaned payment records'
  )
}}

with source as (
    select * from {{ source('bronze', 'payments_raw') }}
),

cleaned as (
    select
        payment_id,
        order_id,
        cast(amount as decimal(12,2))       as amount,
        upper(currency)                     as currency,
        lower(payment_status)               as payment_status,
        lower(gateway)                      as gateway,
        transaction_ref,
        cast(paid_at as timestamp)          as paid_at,
        -- derived
        case when payment_status = 'success'  then true else false end as is_successful,
        case when payment_status = 'refunded' then true else false end as is_refunded,
        cast(_loaded_at as timestamp)       as _loaded_at
    from source
    where payment_id is not null
      and order_id   is not null
)

select * from cleaned