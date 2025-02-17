SELECT *
FROM
    dbt_db.dbt_schema.fct_orders
WHERE
    item_discount_amount > 0