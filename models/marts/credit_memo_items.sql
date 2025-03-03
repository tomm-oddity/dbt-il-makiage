with

credit_memo_items as (
    select * from {{ ref('stg_magento__credit_memo_items') }}
)

select * from credit_memo_items
