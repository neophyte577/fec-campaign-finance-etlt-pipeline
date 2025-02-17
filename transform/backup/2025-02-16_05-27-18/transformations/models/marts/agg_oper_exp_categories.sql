WITH operating_expenditures_by AS (
    SELECT * FROM {{ ref('fct_oper_exp_by_comm') }}
)

SELECT
    CASE
        WHEN category_desc IS NULL THEN 'Other'
        ELSE category_desc
    END AS category,
    SUM(TRANSACTION_AMT) AS transaction_amt
FROM operating_expenditures
GROUP BY category;