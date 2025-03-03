{{
    config(
        materialized='incremental',
        unique_key='entity_id'
    )
}}


with

magento_foundation_quiz_results_wo_additional_data as (
    select * from {{ ref('stg_magento__quiz_results') }}
    where 
        quiz_type = '1'
        {% if is_incremental() %}
        and updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
),

magento_foundation_quiz_results_additional_data as (
    select * from {{ ref('stg_magento__quiz_results_additional_data') }}
    where quiz_result_table = '{{ source('magento', 'quiz_results').identifier }}'
    {% if is_incremental() %}
    and created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

powermatch_app_foundation_quiz_results_wo_answers as (
    select * from {{ ref('stg_powermatch_app__quiz_results') }}
    where 
        quiz_type = 'foundation-2.0'
        {% if is_incremental() %}
        and updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
),

powermatch_app_foundation_quiz_results_answers as (
    select * from {{ ref('int_powermatch_foundation_quiz_results_pivoted_to_quiz_results') }}
    {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

magento_foundation_quiz_results as (
    select
        quiz_results.*,
        additional_data.user_agent,
        additional_data.ip_hash
    from magento_foundation_quiz_results_wo_additional_data as quiz_results
    left join magento_foundation_quiz_results_additional_data as additional_data
        on quiz_results.entity_id = additional_data.quiz_result_id
),

powermatch_app_foundation_quiz_results as (
    select
        quiz_results_metadata.*,
        {{
            dbt_utils.star(
                from=ref('int_powermatch_foundation_quiz_results_pivoted_to_quiz_results'),
                except=['entity_id', 'created_at', 'updated_at']
            )
        }}
    from
        powermatch_app_foundation_quiz_results_wo_answers as quiz_results_metadata
    inner join powermatch_app_foundation_quiz_results_answers as quiz_results_answers
        on quiz_results_metadata.entity_id = quiz_results_answers.entity_id
),

quiz_results as (
    select
        entity_id::text as entity_id,
        created_at,
        updated_at,
        email_hash,
        is_test_email,
        order_id,
        order_increment_id,
        'magento' as quiz_platform,
        variant_type,
        foundation_sku as sku,
        rule_based_wult_sku,
        rule_based_ap_sku,
        user_agent,
        ip_hash,
        utm,
        country_code,
        optimize,
        answer1,
        answer2,
        answer3,
        answer4,
        answer4_1,
        answer4_2,
        answer4_3,
        answer4_4,
        answer4_5,
        answer4_6,
        answer5,
        answer6,
        answer7,
        answer8,
        answer9,
        answer10,
        answer11,
        answer12,
        answer13,
        answer14,
        answer15,
        answer16,
        answer17,
        answer109
    from magento_foundation_quiz_results

    union all

    select
        entity_id,
        created_at,
        updated_at,
        email_hash,
        is_test_email,
        order_id,
        order_increment_id,
        'powermatch_app' as quiz_platform,
        variant_type,
        foundation_sku as sku,
        rule_based_wult_sku,
        null as rule_based_ap_sku,
        user_agent,
        ip_hash,
        utm,
        country_code,
        optimize,
        answer1,
        answer2,
        answer3,
        answer4,
        answer4_1,
        answer4_2,
        answer4_3,
        answer4_4,
        answer4_5,
        answer4_6,
        answer5,
        answer6,
        answer7,
        answer8,
        answer9,
        answer10,
        answer11,
        null::text as answer12,
        answer13,
        answer14,
        answer15,
        answer16,
        answer17,
        answer109
    from powermatch_app_foundation_quiz_results
)


select * from quiz_results
