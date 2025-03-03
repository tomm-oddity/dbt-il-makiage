with

orders as (
    select * from {{ ref('stg_magento__orders') }}
)

select * from orders
