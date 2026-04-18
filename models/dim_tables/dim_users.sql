WITH ratings as(
    select  user_id from {{ref('src_ratings')}}
),
tags as (
    select  user_id from {{ref('src_tags')}}
)
    select * from ratings
    union
    select * from tags 