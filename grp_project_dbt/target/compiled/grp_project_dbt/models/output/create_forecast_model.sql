

-- Create an ML forecast model (ephemeral because it's used inline)
select
    snowflake.ml.forecast(
        input_data => nasa.analytics.session_summary,
        timestamp_colname = 'close_approach_date',
        target_colname ='count of aestroids',
        config_object ={'on_error': 'skip'}
    ) as model