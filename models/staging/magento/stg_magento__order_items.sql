with

source as (

    select * from {{ source('magento', 'order_items') }}

),

renamed as (

    select
        item_id as entity_id,
        order_id,
        parent_item_id,
        sku,
        price,
        created_at,
        updated_at,
        name,
        reason,
        qty_ordered as quantity_ordered
    from source

)

select * from renamed
