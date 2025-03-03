with

source as (

    select * from {{ source('powermatch_app', 'questions') }}

),

renamed as (

    select
        _id as entity_id,
        createdat::timestamp as created_at,
        updatedat::timestamp as updated_at,
        magentoid as magento_id,
        displayid as display_id,
        value as attributes,
        multiselect as is_multi_select
    from source

)

select * from renamed
