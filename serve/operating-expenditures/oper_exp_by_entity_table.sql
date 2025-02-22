SELECT SUM(transaction_amt) AS total, entity_name, purpose
FROM FEC.TRANSFORMED.agg_oper_exp_by_cand
GROUP BY entity_name, purpose
ORDER BY total DESC;