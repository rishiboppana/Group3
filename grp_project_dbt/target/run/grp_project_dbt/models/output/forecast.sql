
  
    

        create or replace transient table nasa.analytics.forecast
         as
        (

SELECT
ts AS date,
ROUND(forecast) AS aestroids_count 
FROM TABLE(aestroids_mdl!forecast(forecasting_periods => 30))
        );
      
  