-- models/staging/stg_customers.sql
{{
  config(
    materialized = 'view',
    description  = 'Cleaned and typed customer records from bronze'
  )
}}

with source as (
    select * from {{ source('bronze', 'customers_raw') }}
),

cleaned as (
    select
        customer_id,
        trim(first_name)                            as first_name,
        trim(last_name)                             as last_name,
        lower(trim(email))                          as email,
        phone,
        trim(city)                                  as city,
        upper(trim(state))                          as state,
        upper(trim(country))                        as country,
        cast(signup_date as date)                   as signup_date,
        cast(is_active as boolean)                  as is_active,
        lower(customer_tier)                        as customer_tier,
        -- derived
        concat(trim(first_name), ' ', trim(last_name)) as full_name,
        cast(_loaded_at as timestamp)               as _loaded_at
    from source
    where customer_id is not null
      and email       is not null
)

select * from cleaned