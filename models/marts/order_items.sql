with

orders_items as (
    select * from {{ ref('stg_magento__order_items') }}
)

select * from orders_items
