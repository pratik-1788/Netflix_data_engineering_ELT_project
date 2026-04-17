    WITH raw_links as(
        select * from netflix_raw_database.raw.links
    )
    select 
    movieId as movie_id,
    imdbId as imdb_id,
    tmdbId as tmdb_id
    from raw_links