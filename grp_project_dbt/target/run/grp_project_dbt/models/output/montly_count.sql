
  
    

        create or replace transient table nasa.analytics.montly_count
         as
        (with __dbt__cte__monthly_count_of_asteroids as (
SELECT
    DATE_TRUNC('month', "CLOSE_APPROACH_DATE") AS Month,
    COUNT(*) AS total_approaches
FROM nasa.raw.nasa_neo_table
GROUP BY 1
ORDER BY 1
) select *
from __dbt__cte__monthly_count_of_asteroids
        );
      
  