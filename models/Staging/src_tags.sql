WITH raw_tags as(
    select * from netflix_raw_database.raw.tags
)
select 
  userId as user_id,
  movieId as movie_id,
  tag,
  TO_TIMESTAMP_LTZ(timestamp) as tag_timestamp
  from raw_tags