WITH individual_contributions AS (
    SELECT * FROM {{ ref('stg_individual_contributions') }}
)

SELECT
    state,
    SUM(transaction_amt) AS sum_indiv_contr
FROM individual_contributions
GROUP BY state