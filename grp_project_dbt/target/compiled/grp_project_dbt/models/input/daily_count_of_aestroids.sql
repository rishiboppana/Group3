select close_approach_date,count(name) as "count of aestroids"
from nasa.raw.nasa_neo_table
group by close_approach_date