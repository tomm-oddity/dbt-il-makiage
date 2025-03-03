{{
    config(
        materialized='incremental',
        unqiue_key='entity_id'
    )
}}

with

foundation_quiz_results_unnested as (
    select
        *,
        null::text as _else_value
    from {{ ref('int_powermatch_foundation_quiz_results_unnested_to_questions') }}
    {% if is_incremental() %}
        where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

final as (
    select
        quiz_result_id as entity_id,
        any_value(created_at) as created_at, -- noqa: CV03
        {{ dbt_utils.pivot(
            'question_magento_id',
            dbt_utils.get_column_values(
                table=ref('int_powermatch_foundation_quiz_results_unnested_to_questions'),
                column='question_magento_id',
                default=[]
            ),
            agg='max',
            then_value='answer',
            else_value='_else_value',
            prefix='answer'
        ) }}
    from foundation_quiz_results_unnested
    group by 1
)

select * from final
