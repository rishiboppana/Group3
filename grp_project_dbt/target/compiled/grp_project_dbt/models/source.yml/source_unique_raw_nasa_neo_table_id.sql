
    
    

select
    id as unique_field,
    count(*) as n_records

from nasa.raw.nasa_neo_table
where id is not null
group by id
having count(*) > 1


