WITH raw_genome_tags as (
    select * from netflix_raw_database.raw.genome_tags
)
select 
  tagId as tag_id,
  tag
  from raw_genome_tags
