{{ config(
    pre_hook="{{ create_forecast_model() }}"
) }}

SELECT
ts AS date,
ROUND(forecast) AS aestroids_count 
FROM TABLE(aestroids_mdl!forecast(forecasting_periods => 30))