{{ config(schema="intermediate", tags=["intermediate"]) }}

with order_items as (
    select order_id, product_sku from {{ ref('stg_order_items') }}
),

customers as (
    select customer_id, email from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
)

select
    orders.*,
    order_items.product_sku as product_sku,
    customers.email as email
from orders
join order_items
    on orders.order_id = order_items.order_id
join customers
    on orders.customer_id = customers.customer_id