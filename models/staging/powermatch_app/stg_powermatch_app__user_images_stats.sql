with

source as (

    select * from {{ source('powermatch_app', 'user_images_stats') }}

),

renamed as (

    select
        _id as user_image_stats_id,
        quizresultid as quiz_result_id,
        createdat::timestamp as created_at,
        imageurl as image_url,
        updatedat::timestamp as updated_at,
        lmdscore as lmd_score,
        lmdconfidence as lmd_confidence_score,
        visionversion as vision_version,
        isood as is_ood,
        oodscore as ood_score
    from source

)

select * from renamed
