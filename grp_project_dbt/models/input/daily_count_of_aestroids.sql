select close_approach_date,count(name) as "count of aestroids"
from {{source('raw','nasa_neo_table')}}
group by close_approach_date