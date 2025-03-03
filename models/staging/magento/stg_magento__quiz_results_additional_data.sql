with

source as (

    select * from {{ source('magento', 'quiz_results_additional_data') }}

),

renamed as (

    select
        data_id as entity_id,
        testresult_id as quiz_result_id,
        testresult_table as quiz_result_table,
        created_at,
        user_agent,
        {{ hash_column('user_ip') }} as ip_hash
    from source

)

select * from renamed
