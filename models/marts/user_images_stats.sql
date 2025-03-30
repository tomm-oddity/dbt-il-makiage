with

source as (

    select * from {{ ref('stg_powermatch_app__user_images_stats') }}

)

select * from source
