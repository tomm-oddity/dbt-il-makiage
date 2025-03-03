{%- set answer_columns = [] -%}
{%- for column in adapter.get_columns_in_relation(source('magento', 'quiz_results')) -%}
    {%- if column.column.startswith('answer') and column.column not in ['answer19', 'answer20'] -%}
        {%- set _ = answer_columns.append("nullif({0}, '') as {0}".format(column.column)) -%}
    {%- endif -%}
{%- endfor -%}

with

source as (

    select * from {{ source('magento', 'quiz_results') }}

),

renamed as (

    select
        entity_id,
        created_at,
        updated_at,
        nullif(order_id, '')::bigint as order_id,
        nullif(order_increment_id, '')::text as order_increment_id,
        nullif(quiz_type, '')::text as quiz_type,
        varient_type as variant_type,
        shade as foundation_sku,
        wult as rule_based_wult_sku,
        ap as rule_based_ap_sku,
        {{ hash_column('answer20') }} as email_hash,
        (
            {{ is_test_email('answer20') }}
        ) as is_test_email,
        answer19 as utm,
        upper(location) as country_code,
        optimize,
        {{ answer_columns | join(',\n        ') }}
    from source
)

select * from renamed
