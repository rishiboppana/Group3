
{% macro create_forecast_model() %}
    create or replace snowflake.ml.forecast aestroids_mdl(
        input_data => table({{ ref('daily_asteroid_count') }}),
        timestamp_colname => 'close_approach_date',
        target_colname => '"count of aestroids"',
        config_object => {'on_error':'skip'}
    );
{% endmacro %}
