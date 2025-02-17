
  
    

        create or replace transient table dbt_db.dbt_schema.int_order_items_summary
         as
        (SELECT 
    order_key,
    SUM(extended_price) AS gross_item_sales_amount,
    SUM(item_discount_amount) AS item_discount_amount
FROM    
    dbt_db.dbt_schema.int_order_items
GROUP BY
    order_key
        );
      
  