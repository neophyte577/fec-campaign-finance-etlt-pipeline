select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        status_code as value_field,
        count(*) as n_records

    from dbt_db.dbt_schema.fct_orders
    group by status_code

)

select *
from all_values
where value_field not in (
    'P','O','F'
)



      
    ) dbt_internal_test