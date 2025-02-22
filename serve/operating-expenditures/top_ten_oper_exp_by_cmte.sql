SELECT SUM(transaction_amt) AS total, cmte_name
FROM FEC.TRANSFORMED.agg_oper_exp_by_comm
GROUP BY cmte_name
ORDER BY total DESC
LIMIT 10;