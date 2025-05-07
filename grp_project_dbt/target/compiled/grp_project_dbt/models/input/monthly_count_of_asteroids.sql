SELECT
    DATE_TRUNC('month', "CLOSE_APPROACH_DATE") AS Month,
    COUNT(*) AS total_approaches
FROM nasa.raw.nasa_neo_table
GROUP BY 1
ORDER BY 1