select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT *
FROM
    dbt_db.dbt_schema.fct_orders
WHERE
    item_discount_amount > 0
      
    ) dbt_internal_test