WITH src_movies as(
    select * from {{ref('src_movies')}}
),
src_link as (
    select * from {{ref ('src_links')}}
)
select 
   s.movie_id,
   INITCAP(TRIM(title)) as movie_title,
   SPLIT(genres, '|') as genre_array,
   genres,
   l.imdb_id,
   l.tmdb_id
from src_movies s
left join src_link l
on s.movie_id=l.movie_id
