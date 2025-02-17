
  create or replace   view dbt_db.dbt_schema.stg_tpch_orders
  
   as (
    SELECT 
    o_orderkey AS order_key,
    o_custkey AS customer_key,
    o_orderstatus AS status_code,
    o_totalprice AS total_price,
    o_orderdate AS order_date
FROM
    snowflake_sample_data.tpch_sf1.orders
  );

