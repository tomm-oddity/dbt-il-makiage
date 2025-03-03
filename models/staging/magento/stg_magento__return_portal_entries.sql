with

source as (

    select * from {{ source('magento', 'return_portal_entries') }}

),

renamed as (

    select
        entity_id,
        created_at,
        updated_at,
        order_id as order_increment_id,
        sku,
        creditmemo_id as creditmemo_increment_id,
        {{ hash_column('email') }} as email_hash,
        (
            {{ is_test_email('email') }}
        ) as is_test_email,
        new_flow as is_new_flow,
        new_flow_result,
        exchange_order_id as exchange_order_increment_id,
        new_sku as exchange_sku,
        case
            when answer3 is null and lower(answer5) like '%exchanged%' then 'exchange'
            else answer3
        end as return_or_exchange,
        answer1,
        answer2,
        answer4,
        answer5,
        nullif(answer6, '') as answer6
    from source

)

select * from renamed
