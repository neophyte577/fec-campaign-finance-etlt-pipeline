select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT *
FROM dbt_db.dbt_schema.fct_orders
WHERE
    DATE(order_date) > CURRENT_DATE()
    OR DATE(order_date) < DATE('1990-01-01')
      
    ) dbt_internal_test