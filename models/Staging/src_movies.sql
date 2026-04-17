WITH raw_movies as(
    select * from netflix_raw_database.raw.movies
)
select movieId as movie_id,
title,genres from raw_movies
