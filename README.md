# End-to-End dbt Cloud + Databricks Industry Project Guide

> **Architecture**: Medallion (Bronze → Silver → Gold) using Unity Catalog on Databricks, with dbt Cloud as the transformation engine. Source data is generated from scratch using Python/SQL scripts.

---

## Table of Contents

1. [Project Overview & Tech Stack](#1-project-overview--tech-stack)
2. [Databricks Setup](#2-databricks-setup)
3. [Source Data Creation (from scratch)](#3-source-data-creation-from-scratch)
4. [dbt Cloud Setup & Databricks Connection](#4-dbt-cloud-setup--databricks-connection)
5. [dbt Project Structure](#5-dbt-project-structure)
6. [dbt_project.yml Configuration](#6-dbt_projectyml-configuration)
7. [Source Declarations (sources.yml)](#7-source-declarations-sourcesyml)
8. [Bronze → Silver: Staging Models](#8-bronze--silver-staging-models)
9. [Silver → Gold: Intermediate & Mart Models](#9-silver--gold-intermediate--mart-models)
10. [dbt Tests & Data Quality](#10-dbt-tests--data-quality)
11. [dbt Macros & Utility Functions](#11-dbt-macros--utility-functions)
12. [dbt Documentation](#12-dbt-documentation)
13. [dbt Cloud Jobs & CI/CD](#13-dbt-cloud-jobs--cicd)
14. [Running the Project](#14-running-the-project)
15. [Best Practices & Industry Standards](#15-best-practices--industry-standards)

---

## 1. Project Overview & Tech Stack

### Business Domain: E-Commerce Analytics

This project models an e-commerce platform with these entities:

- **customers** — registered users
- **orders** — purchase transactions
- **order_items** — line items per order
- **products** — product catalog
- **payments** — payment records per order

### Tech Stack

| Tool | Purpose |
|------|---------|
| Databricks (Unity Catalog) | Data lakehouse, SQL Warehouse, Delta tables |
| dbt Cloud | Transformations, testing, docs, orchestration |
| Python (Databricks notebook) | Source data generation |
| Delta Lake | Storage format for all tables |
| GitHub | Version control, CI/CD trigger |

### Layer Definitions

| Layer | Catalog | Purpose |
|-------|---------|---------|
| Bronze | `ecommerce.bronze` | Raw ingested data, no changes |
| Silver | `ecommerce.silver` | Cleaned, typed, deduplicated |
| Gold | `ecommerce.gold` | Business-ready facts & dimensions |

---

## 2. Databricks Setup

### Step 1: Create a Databricks Workspace

1. Go to [https://databricks.com](https://databricks.com) → Start free trial (AWS/Azure/GCP)
2. Create a workspace named `ecommerce-analytics`

### Step 2: Enable Unity Catalog

In Databricks Admin Console:

```
Account Console → Data → Enable Unity Catalog
Create a metastore → attach to your workspace
```

### Step 3: Create the Catalog and Schemas

Open a Databricks SQL Editor and run:

```sql
-- Create the main catalog
CREATE CATALOG IF NOT EXISTS ecommerce;

-- Create medallion schemas
CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

-- Verify
SHOW SCHEMAS IN ecommerce;
```

### Step 4: Create a SQL Warehouse (for dbt)

```
Databricks UI → SQL Warehouses → Create warehouse
  Name: dbt-warehouse
  Size: Small (2 DBU) — enough for dev
  Auto-stop: 10 minutes
  Type: Serverless (recommended)
```

Copy the **HTTP Path** — you'll need it for dbt Cloud connection.
Format: `/sql/1.0/warehouses/xxxxxxxxxxxxxxxx`

### Step 5: Create a Service Principal (for dbt Cloud auth)

```
Account Console → Service Principals → Add service principal
  Name: dbt-cloud-sp

# Generate a personal access token (PAT):
Databricks UI → User Settings → Developer → Access Tokens → Generate token
  Name: dbt-cloud-token
  Lifetime: 90 days (or longer for prod)
```

---

## 3. Source Data Creation (from scratch)

### Step 1: Open a Databricks Notebook

Go to Databricks UI → Workspace → Create → Notebook
Name: `00_generate_source_data`
Language: Python
Attach to: any All-Purpose Cluster

### Step 2: Install Faker (for realistic data)

```python
# Cell 1 — install dependencies
%pip install faker
```

### Step 3: Generate All Source Tables

```python
# Cell 2 — imports
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import uuid

fake = Faker()
random.seed(42)
Faker.seed(42)

spark = SparkSession.builder.getOrCreate()

print("Libraries loaded successfully")
```

```python
# Cell 3 — generate CUSTOMERS (5,000 rows)
def generate_customers(n=5000):
    customers = []
    for i in range(1, n + 1):
        signup = fake.date_time_between(start_date="-3y", end_date="now")
        customers.append({
            "customer_id":   str(uuid.uuid4()),
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "email":         fake.email(),
            "phone":         fake.phone_number(),
            "city":          fake.city(),
            "state":         fake.state_abbr(),
            "country":       "US",
            "signup_date":   signup.strftime("%Y-%m-%d"),
            "is_active":     random.choice([True, True, True, False]),  # 75% active
            "customer_tier": random.choice(["bronze", "silver", "gold", "platinum"]),
            "_loaded_at":    datetime.now().isoformat()
        })
    return customers

customers_data = generate_customers()
customers_df = spark.createDataFrame(customers_data)
customers_df.write.format("delta").mode("overwrite").saveAsTable("ecommerce.bronze.customers_raw")
print(f"Customers: {customers_df.count()} rows written to ecommerce.bronze.customers_raw")
```

```python
# Cell 4 — generate PRODUCTS (500 rows)
CATEGORIES = {
    "Electronics":    ["Laptop", "Phone", "Tablet", "Headphones", "Smartwatch", "Camera"],
    "Clothing":       ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Cap"],
    "Home & Kitchen": ["Blender", "Coffee Maker", "Toaster", "Vacuum", "Lamp", "Pillow"],
    "Books":          ["Fiction Novel", "Biography", "Self-Help", "Cookbook", "Science"],
    "Sports":         ["Yoga Mat", "Dumbbells", "Running Shoes", "Bicycle", "Helmet"]
}

def generate_products(n=500):
    products = []
    for i in range(1, n + 1):
        category    = random.choice(list(CATEGORIES.keys()))
        item_name   = random.choice(CATEGORIES[category])
        base_price  = round(random.uniform(5.99, 999.99), 2)
        products.append({
            "product_id":    str(uuid.uuid4()),
            "product_name":  f"{fake.company().split()[0]} {item_name}",
            "category":      category,
            "subcategory":   item_name,
            "brand":         fake.company().split()[0],
            "unit_price":    base_price,
            "cost_price":    round(base_price * random.uniform(0.3, 0.7), 2),
            "sku":           fake.bothify(text="SKU-???-####").upper(),
            "is_active":     random.choice([True, True, True, True, False]),
            "created_at":    fake.date_time_between(start_date="-4y", end_date="-6m").isoformat(),
            "_loaded_at":    datetime.now().isoformat()
        })
    return products

products_data = generate_products()
products_df = spark.createDataFrame(products_data)
products_df.write.format("delta").mode("overwrite").saveAsTable("ecommerce.bronze.products_raw")
print(f"Products: {products_df.count()} rows written to ecommerce.bronze.products_raw")
```

```python
# Cell 5 — generate ORDERS & ORDER_ITEMS (50,000 orders)
# First collect customer_ids and product_ids
customer_ids = [row.customer_id for row in spark.table("ecommerce.bronze.customers_raw").select("customer_id").collect()]
product_rows = spark.table("ecommerce.bronze.products_raw").select("product_id", "unit_price").collect()
product_map  = {row.product_id: row.unit_price for row in product_rows}
product_ids  = list(product_map.keys())

ORDER_STATUSES  = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
STATUS_WEIGHTS  = [0.05, 0.10, 0.15, 0.55, 0.10, 0.05]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]

def generate_orders(n=50000):
    orders      = []
    order_items = []

    for i in range(1, n + 1):
        order_id     = str(uuid.uuid4())
        customer_id  = random.choice(customer_ids)
        order_date   = fake.date_time_between(start_date="-2y", end_date="now")
        status       = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]
        num_items    = random.randint(1, 6)
        item_product_ids = random.sample(product_ids, min(num_items, len(product_ids)))

        order_total = 0.0
        for pid in item_product_ids:
            qty      = random.randint(1, 5)
            price    = product_map[pid]
            discount = round(random.uniform(0, 0.25), 2)
            line_total = round(qty * price * (1 - discount), 2)
            order_total += line_total
            order_items.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id":      order_id,
                "product_id":    pid,
                "quantity":      qty,
                "unit_price":    price,
                "discount_pct":  discount,
                "line_total":    line_total,
                "_loaded_at":    datetime.now().isoformat()
            })

        shipped_at   = None
        delivered_at = None
        if status in ["shipped", "delivered"]:
            shipped_at   = (order_date + timedelta(days=random.randint(1, 3))).isoformat()
        if status == "delivered":
            delivered_at = (order_date + timedelta(days=random.randint(4, 10))).isoformat()

        orders.append({
            "order_id":       order_id,
            "customer_id":    customer_id,
            "order_date":     order_date.strftime("%Y-%m-%d"),
            "order_status":   status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "order_total":    round(order_total, 2),
            "shipping_cost":  round(random.uniform(0, 15.99), 2),
            "shipped_at":     shipped_at,
            "delivered_at":   delivered_at,
            "promo_code":     fake.bothify("PROMO-????") if random.random() < 0.2 else None,
            "_loaded_at":     datetime.now().isoformat()
        })

    return orders, order_items

orders_data, order_items_data = generate_orders()

orders_df = spark.createDataFrame(orders_data)
orders_df.write.format("delta").mode("overwrite").saveAsTable("ecommerce.bronze.orders_raw")
print(f"Orders: {orders_df.count()} rows written to ecommerce.bronze.orders_raw")

order_items_df = spark.createDataFrame(order_items_data)
order_items_df.write.format("delta").mode("overwrite").saveAsTable("ecommerce.bronze.order_items_raw")
print(f"Order items: {order_items_df.count()} rows written to ecommerce.bronze.order_items_raw")
```

```python
# Cell 6 — generate PAYMENTS (one per delivered/confirmed order)
orders_for_payment = spark.sql("""
    SELECT order_id, order_total, order_date
    FROM ecommerce.bronze.orders_raw
    WHERE order_status IN ('confirmed', 'shipped', 'delivered')
""").collect()

PAYMENT_STATUSES = ["success", "success", "success", "failed", "refunded"]

def generate_payments(orders):
    payments = []
    for row in orders:
        payments.append({
            "payment_id":     str(uuid.uuid4()),
            "order_id":       row.order_id,
            "amount":         row.order_total,
            "currency":       "USD",
            "payment_status": random.choice(PAYMENT_STATUSES),
            "gateway":        random.choice(["stripe", "paypal", "braintree", "adyen"]),
            "transaction_ref": fake.bothify("TXN-########"),
            "paid_at":        (datetime.strptime(row.order_date, "%Y-%m-%d") + timedelta(hours=random.randint(0, 2))).isoformat(),
            "_loaded_at":     datetime.now().isoformat()
        })
    return payments

payments_data = generate_payments(orders_for_payment)
payments_df = spark.createDataFrame(payments_data)
payments_df.write.format("delta").mode("overwrite").saveAsTable("ecommerce.bronze.payments_raw")
print(f"Payments: {payments_df.count()} rows written to ecommerce.bronze.payments_raw")
```

```python
# Cell 7 — verify all bronze tables
tables = ["customers_raw", "orders_raw", "order_items_raw", "products_raw", "payments_raw"]
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM ecommerce.bronze.{t}").collect()[0].cnt
    print(f"ecommerce.bronze.{t}: {count:,} rows")
```

Expected output:
```
ecommerce.bronze.customers_raw:   5,000 rows
ecommerce.bronze.orders_raw:     50,000 rows
ecommerce.bronze.order_items_raw: ~150,000 rows
ecommerce.bronze.products_raw:      500 rows
ecommerce.bronze.payments_raw:    ~38,000 rows
```

---

## 4. dbt Cloud Setup & Databricks Connection

### Step 1: Create a dbt Cloud Account

1. Go to [https://cloud.getdbt.com](https://cloud.getdbt.com) → Sign up (free developer plan)
2. Create a new project: `ecommerce-analytics`

### Step 2: Connect dbt Cloud to Databricks

In dbt Cloud → Project Settings → Connections → New Connection:

```
Connection type: Databricks
Connection name: databricks-ecommerce

Server hostname: <your-databricks-workspace>.azuredatabricks.net
                 (find in Databricks URL: adb-XXXXXX.XX.azuredatabricks.net)

HTTP Path:      /sql/1.0/warehouses/XXXXXXXXXXXXXXXX
                (from SQL Warehouse → Connection details)

Catalog:        ecommerce

Token:          <your-personal-access-token>
```

### Step 3: Set Up Development Credentials

dbt Cloud → Your Profile → Credentials → Add credentials for the project:

```
Schema:  dev_<your_name>      # e.g. dev_john — keeps dev isolated
Catalog: ecommerce
```

### Step 4: Connect to GitHub

dbt Cloud → Project Settings → Repository → Connect to GitHub:

1. Authorize dbt Cloud on GitHub
2. Create a new repo: `ecommerce-dbt`
3. Select it in dbt Cloud

### Step 5: Initialize the dbt Project

In dbt Cloud IDE → Initialize project:

```bash
# This creates the default project scaffold
# You'll see: models/, tests/, macros/, dbt_project.yml
```

---

## 5. dbt Project Structure

```
ecommerce-dbt/
├── dbt_project.yml                  # Main project config
├── packages.yml                     # dbt packages (dbt-utils, etc.)
├── profiles.yml                     # (local only, not committed)
│
├── models/
│   ├── staging/                     # Bronze → Silver (1:1 source mapping)
│   │   ├── _sources.yml             # Source declarations
│   │   ├── _staging.yml             # Staging model docs & tests
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_products.sql
│   │   └── stg_payments.sql
│   │
│   ├── intermediate/                # Silver → intermediate joins
│   │   ├── _intermediate.yml
│   │   ├── int_orders_with_items.sql
│   │   └── int_customer_orders.sql
│   │
│   └── marts/                       # Gold: facts & dimensions
│       ├── _marts.yml
│       ├── fct_orders.sql
│       ├── fct_order_items.sql
│       ├── dim_customers.sql
│       └── dim_products.sql
│
├── macros/
│   ├── cents_to_dollars.sql
│   ├── clean_email.sql
│   └── generate_surrogate_key.sql
│
├── tests/
│   └── assert_payment_amount_positive.sql   # singular tests
│
├── seeds/
│   └── country_codes.csv
│
└── analyses/
    └── revenue_by_category.sql
```

---

## 6. dbt_project.yml Configuration

```yaml
# dbt_project.yml
name: 'ecommerce_analytics'
version: '1.0.0'
config-version: 2

profile: 'ecommerce_analytics'

model-paths:   ["models"]
analysis-paths: ["analyses"]
test-paths:    ["tests"]
seed-paths:    ["seeds"]
macro-paths:   ["macros"]
snapshot-paths: ["snapshots"]

target-path:   "target"
clean-targets: ["target", "dbt_packages"]

models:
  ecommerce_analytics:

    staging:
      +materialized: view          # staging = views (lightweight)
      +schema: silver              # writes to ecommerce.silver
      +tags: ["staging"]

    intermediate:
      +materialized: ephemeral     # not persisted, inlined into downstream
      +tags: ["intermediate"]

    marts:
      +materialized: table         # gold = physical Delta tables
      +schema: gold                # writes to ecommerce.gold
      +tags: ["marts"]
      fct_orders:
        +materialized: incremental # large fact tables = incremental
        +incremental_strategy: merge
        +unique_key: order_id
      fct_order_items:
        +materialized: incremental
        +incremental_strategy: merge
        +unique_key: order_item_id

vars:
  start_date: '2022-01-01'
```

---

## 7. Source Declarations (sources.yml)

Create `models/staging/_sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    description: "Raw ingested data from the e-commerce platform"
    catalog: ecommerce
    schema: bronze
    tags: ["bronze", "raw"]

    tables:
      - name: customers_raw
        description: "Raw customer registrations"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
        columns:
          - name: customer_id
            description: "UUID primary key"
            tests:
              - unique
              - not_null
          - name: email
            tests:
              - unique
              - not_null

      - name: orders_raw
        description: "Raw order transactions"
        loaded_at_field: _loaded_at
        freshness:
          warn_after:  {count: 12, period: hour}
          error_after: {count: 24, period: hour}
        columns:
          - name: order_id
            tests: [unique, not_null]
          - name: customer_id
            tests: [not_null]
          - name: order_status
            tests:
              - accepted_values:
                  values: ['pending','confirmed','shipped','delivered','cancelled','returned']

      - name: order_items_raw
        description: "Line items for each order"
        columns:
          - name: order_item_id
            tests: [unique, not_null]
          - name: order_id
            tests: [not_null]
          - name: product_id
            tests: [not_null]
          - name: quantity
            tests:
              - dbt_utils.expression_is_true:
                  expression: ">= 1"

      - name: products_raw
        description: "Product catalog"
        columns:
          - name: product_id
            tests: [unique, not_null]
          - name: unit_price
            tests:
              - dbt_utils.expression_is_true:
                  expression: "> 0"

      - name: payments_raw
        description: "Payment records"
        columns:
          - name: payment_id
            tests: [unique, not_null]
          - name: order_id
            tests: [not_null]
          - name: payment_status
            tests:
              - accepted_values:
                  values: ['success', 'failed', 'refunded', 'pending']
```

---

## 8. Bronze → Silver: Staging Models

### stg_customers.sql

```sql
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
```

### stg_orders.sql

```sql
-- models/staging/stg_orders.sql
{{
  config(
    materialized = 'view',
    description  = 'Cleaned order records with status normalization'
  )
}}

with source as (
    select * from {{ source('bronze', 'orders_raw') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date)                    as order_date,
        lower(order_status)                         as order_status,
        lower(payment_method)                       as payment_method,
        cast(order_total     as decimal(12,2))      as order_total,
        cast(shipping_cost   as decimal(8,2))       as shipping_cost,
        cast(shipped_at   as timestamp)             as shipped_at,
        cast(delivered_at as timestamp)             as delivered_at,
        promo_code,
        -- derived flags
        case when order_status = 'delivered'  then true else false end as is_delivered,
        case when order_status = 'cancelled'  then true else false end as is_cancelled,
        case when order_status = 'returned'   then true else false end as is_returned,
        case when promo_code  is not null      then true else false end as has_promo,
        cast(_loaded_at as timestamp)               as _loaded_at
    from source
    where order_id   is not null
      and customer_id is not null
)

select * from cleaned
```

### stg_order_items.sql

```sql
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
```

### stg_products.sql

```sql
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
```

### stg_payments.sql

```sql
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
```

---

## 9. Silver → Gold: Intermediate & Mart Models

### Intermediate: int_orders_with_items.sql

```sql
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
```

### Gold Fact: fct_orders.sql

```sql
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
```

### Gold Dimension: dim_customers.sql

```sql
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
```

### Gold Dimension: dim_products.sql

```sql
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
```

---

## 10. dbt Tests & Data Quality

### Schema Tests in `_marts.yml`

```yaml
# models/marts/_marts.yml
version: 2

models:
  - name: fct_orders
    description: "Core order fact table — one row per order"
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: order_total
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: order_status
        tests:
          - accepted_values:
              values: ['pending','confirmed','shipped','delivered','cancelled','returned']
      - name: order_date
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= '2020-01-01'"

  - name: dim_customers
    description: "Customer dimension with lifetime metrics"
    columns:
      - name: customer_id
        tests: [unique, not_null]
      - name: email
        tests: [unique, not_null]
      - name: value_segment
        tests:
          - accepted_values:
              values: ['vip','high_value','mid_value','low_value','no_purchase']
      - name: total_orders
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"

  - name: dim_products
    description: "Product dimension with sales metrics"
    columns:
      - name: product_id
        tests: [unique, not_null]
      - name: margin_pct
        tests:
          - dbt_utils.expression_is_true:
              expression: "between 0 and 100"
      - name: sales_tier
        tests:
          - accepted_values:
              values: ['top_seller','good_seller','slow_mover','no_sales']
```

### Singular Test: Payment Amount Validation

```sql
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
```

### Singular Test: Order Total Consistency

```sql
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
```

---

## 11. dbt Macros & Utility Functions

### macros/clean_email.sql

```sql
{% macro clean_email(column_name) %}
    lower(trim(regexp_replace({{ column_name }}, '\\s+', '')))
{% endmacro %}
```

Usage in models:
```sql
{{ clean_email('email') }} as email
```

### macros/date_spine.sql

```sql
{% macro date_spine(start_date, end_date) %}
    select explode(sequence(
        to_date('{{ start_date }}'),
        to_date('{{ end_date }}'),
        interval 1 day
    )) as date_day
{% endmacro %}
```

### macros/cents_to_dollars.sql

```sql
{% macro cents_to_dollars(column_name, precision=2) %}
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
```

### macros/is_weekend.sql

```sql
{% macro is_weekend(date_column) %}
    case when dayofweek({{ date_column }}) in (1, 7) then true else false end
{% endmacro %}
```

---

## 12. dbt Documentation

### Add Model Descriptions in YAML

Add rich descriptions to every model's `_*.yml` file. Example:

```yaml
models:
  - name: fct_orders
    description: >
      Core order fact table containing one record per order.
      Includes enriched financial metrics, item counts, shipping timelines,
      and payment status. This is the primary table for revenue reporting.
      Materialized as an incremental Delta table in the gold layer.

    meta:
      owner: "data-team@company.com"
      domain: "commerce"
      sla: "daily refresh by 6am UTC"
```

### Generate and Serve Docs

```bash
# In dbt Cloud IDE terminal or locally:
dbt docs generate
dbt docs serve      # opens docs at localhost:8080 locally
```

In dbt Cloud: go to **Deploy → Documentation** to publish your docs site automatically.

---

## 13. dbt Cloud Jobs & CI/CD

### packages.yml — Install dbt-utils

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

```bash
dbt deps   # install packages
```

### Production Job Setup

In dbt Cloud → Deploy → Jobs → New Job:

**Job 1: Full Refresh (weekly)**
```
Name:     weekly-full-refresh
Command:  dbt build --full-refresh --select staging+ marts
Schedule: Every Sunday at 2:00 AM UTC
Environment: production
```

**Job 2: Daily Incremental Run**
```
Name:     daily-incremental
Commands:
  dbt source freshness
  dbt build --select staging+ fct_orders fct_order_items
  dbt test --select marts
Schedule: Every day at 5:00 AM UTC
```

**Job 3: CI Check (on PR)**
```
Name:    ci-slim-check
Trigger: Pull Request
Command: dbt build --select state:modified+ --defer --state ./prod-artifacts
```

### GitHub Actions for CI (optional)

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Trigger dbt Cloud CI Job
        run: |
          curl -X POST \
            -H "Authorization: Token ${{ secrets.DBT_CLOUD_API_TOKEN }}" \
            -H "Content-Type: application/json" \
            -d '{"cause": "CI from GitHub PR #${{ github.event.pull_request.number }}"}' \
            https://cloud.getdbt.com/api/v2/accounts/${{ secrets.DBT_ACCOUNT_ID }}/jobs/${{ secrets.DBT_JOB_ID }}/run/
```

---

## 14. Running the Project

### Development Workflow

```bash
# 1. Install packages
dbt deps

# 2. Compile (check SQL without running)
dbt compile

# 3. Run all models (dev schema)
dbt run

# 4. Run tests
dbt test

# 5. Run + test together (recommended)
dbt build

# 6. Run specific layers
dbt run --select staging
dbt run --select marts
dbt run --select fct_orders+    # fct_orders and all downstream

# 7. Run with full refresh (rebuild tables from scratch)
dbt run --full-refresh --select marts

# 8. Check source freshness
dbt source freshness

# 9. Generate lineage + docs
dbt docs generate
dbt docs serve

# 10. View compiled SQL (in target/compiled/)
cat target/compiled/ecommerce_analytics/models/marts/fct_orders.sql
```

### Validating in Databricks

After a successful `dbt run`, validate in Databricks SQL Editor:

```sql
-- Check gold tables were created
SHOW TABLES IN ecommerce.gold;

-- Preview facts
SELECT * FROM ecommerce.gold.fct_orders LIMIT 10;

-- Quick revenue check
SELECT
    order_year,
    order_month,
    count(*)        as total_orders,
    sum(order_total) as total_revenue,
    avg(order_total) as avg_order_value
FROM ecommerce.gold.fct_orders
WHERE NOT is_cancelled
GROUP BY 1, 2
ORDER BY 1, 2;

-- Customer segments
SELECT value_segment, count(*) as customers, avg(lifetime_revenue) as avg_ltv
FROM ecommerce.gold.dim_customers
GROUP BY 1
ORDER BY avg_ltv DESC;
```

---

## 15. Best Practices & Industry Standards

### Naming Conventions

| Prefix | Layer | Example |
|--------|-------|---------|
| `stg_` | Staging (silver) | `stg_orders` |
| `int_` | Intermediate | `int_orders_with_items` |
| `fct_` | Fact table (gold) | `fct_orders` |
| `dim_` | Dimension (gold) | `dim_customers` |
| `rpt_` | Report-level aggregate | `rpt_monthly_revenue` |

### Materialization Strategy

| Model type | Materialization | Why |
|------------|----------------|-----|
| Staging | `view` | Always fresh, no storage cost |
| Intermediate | `ephemeral` | Reusable CTEs, not persisted |
| Small dimensions | `table` | Fast queries, small data |
| Large facts | `incremental` | Efficient updates only |
| One-time aggregates | `table` + manual refresh | Stability |

### Column Standards

Always include these in every model:
- A clear primary key column
- `_loaded_at` timestamp for lineage tracking
- Audit columns for incremental models (`updated_at`)

### Code Quality Checklist

- [ ] Every model has a description in YAML
- [ ] Every primary key has `unique` + `not_null` tests
- [ ] All foreign keys have `relationships` tests
- [ ] `accepted_values` on every enum/status column
- [ ] Source freshness configured for all raw tables
- [ ] No hardcoded schema/database names (use `ref()` and `source()`)
- [ ] Incremental models have proper `is_incremental()` filter
- [ ] Models are tagged for selective job runs

### Folder Discipline

- One model = one SQL file
- Keep staging 1:1 with source tables
- No business logic in staging — cleaning only
- No raw source references in marts (always via `ref()`)
- Group related mart models into subfolders: `marts/finance/`, `marts/product/`

---

*Built with dbt Cloud + Databricks Unity Catalog | Medallion Architecture | ~200,000+ rows of synthetic e-commerce data*
