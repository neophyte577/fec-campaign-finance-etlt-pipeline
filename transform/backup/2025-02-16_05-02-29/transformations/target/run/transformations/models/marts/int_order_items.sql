
  
    

        create or replace transient table dbt_db.dbt_schema.int_order_items
         as
        (SELECT 
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    
    (-1 * line_item.extended_price * line_item.discount_percentage)::decimal(16, 2)
 as item_discount_amount
FROM    
    dbt_db.dbt_schema.stg_tpch_orders AS orders
JOIN
    dbt_db.dbt_schema.stg_tpch_line_items AS line_item
        ON orders.order_key = line_item.order_key
ORDER BY
    orders.order_date
        );
      
  