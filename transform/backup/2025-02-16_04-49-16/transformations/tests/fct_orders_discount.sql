SELECT *
FROM
    {{ref('fct_orders')}}
WHERE
    item_discount_amount > 0