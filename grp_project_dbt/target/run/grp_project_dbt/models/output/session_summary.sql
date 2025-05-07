
  
    

        create or replace transient table nasa.analytics.session_summary
         as
        (with __dbt__cte__count_of_aestroids as (
select close_approach_date,count(name) as "count of aestroids"
from nasa.raw.nasa_neo_table
group by close_approach_date
) select * 
from __dbt__cte__count_of_aestroids
        );
      
  