{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        unique_key=['quiz_result_id', 'question_magento_id'],
        event_time='created_at',
        batch_size='day',
        begin='2022-09-08',
        full_refresh=false
    )
}}

with

_quiz_results as (
    select * from {{ ref('stg_powermatch_app__quiz_results') }}
),

questions as (
    select * from {{ ref('stg_powermatch_app__questions') }}
),

answers as (
    select * from {{ ref('stg_powermatch_app__answers') }}
),

foundation_quiz_results as (
    select * from _quiz_results
    where
        quiz_type = 'foundation-2.0'
),

quiz_results_unnested_single_select_answers as (
    select
        quiz_results.entity_id as quiz_result_id,
        answer_objects.questionid::varchar(24) as question_id,
        quiz_results.created_at,
        answers.magento_id as answer
    from
        foundation_quiz_results as quiz_results,
        quiz_results.answers as answer_objects,
        answer_objects.answerids as answer_id at answer_ix,
        questions,
        answers
    where
        questions.entity_id = answer_objects.questionid::varchar(24)
        -- Only non multi-select questions
        and not questions.is_multi_select is true
        -- Only if no answer value is provided
        and answer_objects.value is null
        -- Then only take the last answer ID
        and answer_ix = get_array_length(answer_objects.answerids) - 1 -- noqa: RF02
        and answers.entity_id = answer_id::varchar(24) -- noqa: RF02
),

quiz_results_unnested_multi_select_answers as (
    select
        quiz_results.entity_id as quiz_result_id,
        answer_objects.questionid::varchar(24) as question_id,
        any_value(quiz_results.created_at) as created_at,
        listagg(distinct answers.magento_id, ',') within group (
            order by answer_ix -- noqa: RF02
        ) as answer
    from
        foundation_quiz_results as quiz_results,
        quiz_results.answers as answer_objects,
        answer_objects.answerids as answer_id at answer_ix,
        questions,
        answers
    where
        questions.entity_id = answer_objects.questionid::varchar(24)
        -- Only multi-select questions
        and questions.is_multi_select is true
        and answers.entity_id = answer_id::varchar(24) -- noqa: RF02
    group by 1, 2
),

quiz_results_unnested_values as (
    select
        quiz_results.entity_id as quiz_result_id,
        answer_objects.questionid::varchar(24) as question_id,
        quiz_results.created_at,
        case
            when
                lower(questions.display_id) like '%email%'
                or lower(questions.display_id) like '%name%'
                or answer_objects.value::text like '%@%'
            then {{ hash_column('answer_objects.value::text') }}
            else answer_objects.value::text
        end as answer
    from
        foundation_quiz_results as quiz_results,
        quiz_results.answers as answer_objects,
        questions
    where
        answer_objects.value is not null
        and (
            answer_objects.answerids is null
            or get_array_length(answer_objects.answerids) = 0
        )
        and questions.entity_id = answer_objects.questionid::varchar(24)
),

quiz_results_unnested_union as (
    select * from quiz_results_unnested_single_select_answers
    union all
    select * from quiz_results_unnested_multi_select_answers
    union all
    select * from quiz_results_unnested_values
)

select
    quiz_results_unnested_union.quiz_result_id,
    quiz_results_unnested_union.created_at,
    questions.magento_id as question_magento_id,
    quiz_results_unnested_union.answer
from quiz_results_unnested_union
inner join
    questions
    on quiz_results_unnested_union.question_id = questions.entity_id
