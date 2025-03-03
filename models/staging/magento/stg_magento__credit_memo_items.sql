with

source as (

    select * from {{ source('magento', 'credit_memo_items') }}

),

renamed as (

    select
        entity_id,
        parent_id as credit_memo_id,
        order_item_id,
        qty as quantity,
        price,
        sku,
        reason
    from source

)

select * from renamed
