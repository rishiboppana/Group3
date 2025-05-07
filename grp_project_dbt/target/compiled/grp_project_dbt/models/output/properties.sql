with __dbt__cte__physical_properties as (
SELECT
    id,
    name,
    close_approach_date,
    CASE
        WHEN estimated_diameter_max_km < 0.1 THEN 'Small'
        WHEN estimated_diameter_max_km < 0.5 THEN 'Medium'
        ELSE 'Large'
    END AS size_category,
    CASE
        WHEN relative_velocity_kmh > 60000 THEN 'High'
        WHEN relative_velocity_kmh > 30000 THEN 'Medium'
        ELSE 'Low'
    END AS speed_category
FROM nasa.raw.nasa_neo_table
) select * 
from __dbt__cte__physical_properties