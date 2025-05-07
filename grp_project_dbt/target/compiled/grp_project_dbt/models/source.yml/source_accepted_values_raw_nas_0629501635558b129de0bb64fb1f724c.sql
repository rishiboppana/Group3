
    
    

with all_values as (

    select
        is_sentry_object as value_field,
        count(*) as n_records

    from nasa.raw.nasa_neo_table
    group by is_sentry_object

)

select *
from all_values
where value_field not in (
    'True','False'
)


