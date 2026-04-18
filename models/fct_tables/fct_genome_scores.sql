WITH src_score as (
    select * from {{ref ('src_genome_score')}}
)
select 
   movie_id,
   tag_id,
   round(relevance,4) as relevance_score
from src_score
where relevance>0
