with

source as (

    select * from {{ source('magento', 'credit_memos') }}

),

renamed as (

    select
        entity_id,
        increment_id,
        created_at,
        updated_at,
        store_id,
        order_id
    from source

)

select * from renamed
