WITH  src_tags as (
    select * from {{ref('src_tags')}}
)
select 
  user_id,
  movie_id,
  tag,
  tag_timestamp
  from src_tags