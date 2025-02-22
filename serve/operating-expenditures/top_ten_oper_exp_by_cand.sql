SELECT SUM(transaction_amt) AS total, cand_name 
FROM FEC.TRANSFORMED.agg_oper_exp_by_cand
GROUP BY cand_name
ORDER BY total DESC
LIMIT 10;