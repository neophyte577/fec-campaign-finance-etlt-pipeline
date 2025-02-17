select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select order_key as from_field
    from dbt_db.dbt_schema.fct_orders
    where order_key is not null
),

parent as (
    select order_key as to_field
    from dbt_db.dbt_schema.stg_tpch_orders
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test