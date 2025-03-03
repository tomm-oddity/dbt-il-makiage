with

source as (

    select * from {{ source('magento', 'order_addresses') }}

),

renamed as (

    select
        entity_id,
        address_type,
        parent_id as order_id,
        country_id as country_code,
        region,
        city,
        {{ hash_column('postcode', lower=False) }} as postcode_hash
    from source
)

select * from renamed
