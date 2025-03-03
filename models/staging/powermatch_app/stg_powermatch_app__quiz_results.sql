with

source as (

    select * from {{ source('powermatch_app', 'quiz_results') }}

),

renamed as (

    select
        _id as entity_id,
        createdat::timestamp as created_at,
        updatedat::timestamp as updated_at,
        attributes.type::varchar(256) as quiz_type,
        {{ hash_column('attributes.user.email::text') }} as email_hash,
        {{ is_test_email('attributes.user.email::text') }} as is_test_email,
        attributes.user.agent::text as user_agent,
        attributes.match.showntype::text as variant_type,
        attributes.match.shownsku::text as foundation_sku,
        attributes.match.wultsku::text as rule_based_wult_sku,
        upper(left(attributes.country::text, 2))::varchar(2) as country_code,
        attributes.experiment as optimize,
        attributes.utm::text as utm,
        {{ hash_column('attributes.user.ip::text') }} as ip_hash,
        coalesce(orderid[0]::bigint, originorderid::bigint) as order_id,
        incrementid[0]::text as order_increment_id,
        value as answers
    from source

)

select * from renamed
