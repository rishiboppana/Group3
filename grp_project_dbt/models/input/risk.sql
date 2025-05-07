SELECT
    id,
    miss_distance_km,
    estimated_diameter_max_km,
    relative_velocity_kmh,
    close_approach_date,
    (estimated_diameter_max_km* relative_velocity_kmh) / NULLIF(miss_distance_km, 0) AS risk_score,
    CASE
        WHEN (estimated_diameter_max_km * relative_velocity_kmh) / NULLIF(miss_distance_km, 0) > 1000 THEN 'High'
        WHEN (estimated_diameter_max_km * relative_velocity_kmh) / NULLIF(miss_distance_km, 0) > 500 THEN 'Medium'
        ELSE 'Low'
    END AS risk_level
FROM {{source('raw',"nasa_neo_table")}}
ORDER BY CLOSE_APPROACH_DATE