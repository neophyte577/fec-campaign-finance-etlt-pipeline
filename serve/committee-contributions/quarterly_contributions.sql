SELECT DATE_TRUNC('QUARTER', transaction_dt) AS quarter, SUM(transaction_amt)
FROM FEC.TRANSFORMED.fct_committee_contributions
WHERE {{ Candidate }}
    AND YEAR(transaction_dt) = 2024
GROUP BY quarter
ORDER BY quarter ASC;