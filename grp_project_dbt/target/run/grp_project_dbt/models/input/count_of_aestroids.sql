
  create or replace   view user_db_lobster1.analytics.count_of_aestroids
  
   as (
    select close_approach_date,count(name) as "count of aestroids"
from nasa.raw.nasa_neo_table
group by close_approach_date
  );

