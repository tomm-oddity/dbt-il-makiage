with

credit_memos as (
    select * from {{ ref('stg_magento__credit_memos') }}
)

select * from credit_memos
