SELECT SUM(transaction_amt) as total, category
FROM FEC.TRANSFORMED.agg_oper_exp_categories
GROUP BY category
WHERE category != 'Other';