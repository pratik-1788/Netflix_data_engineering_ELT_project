WITH raw_genome_score as(
    select * from netflix_raw_database.raw.genome_scores
)
select 
 movieId as movie_id,
 tagId as tag_id,
 relevance
 from raw_genome_score