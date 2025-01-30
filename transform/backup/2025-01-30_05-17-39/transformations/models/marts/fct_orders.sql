SELECT 
    orders.*,
    order_items_summary.gross_item_sales_amount,
    order_items_summary.item_discount_amount
FROM
    {{ref('stg_tpch_orders')}} AS orders
JOIN 
    {{ref('int_order_items_summary')}} AS order_items_summary ON orders.order_key = order_items_summary.order_key 
ORDER BY 
    order_date