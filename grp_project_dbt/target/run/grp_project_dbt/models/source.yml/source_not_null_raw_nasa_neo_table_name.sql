select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select name
from nasa.raw.nasa_neo_table
where name is null



      
    ) dbt_internal_test