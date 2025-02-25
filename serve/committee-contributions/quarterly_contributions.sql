SELECT SUM(transaction_amt) AS total, cmte_name
FROM FEC.TRANSFORMED.fct_committee_contributions
WHERE {{ Candidate }}
    AND transaction_tp_code NOT IN ('24A', '24N')
GROUP BY cmte_name
ORDER BY total DESC
LIMIT 10;