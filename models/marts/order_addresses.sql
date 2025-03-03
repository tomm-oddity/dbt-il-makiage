with

order_addresses as (
    select * from {{ ref('stg_magento__order_addresses') }}
)

select * from order_addresses
