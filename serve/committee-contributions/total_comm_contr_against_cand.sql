SELECT SUM(transaction_amt)
FROM FEC.TRANSFORMED.fct_committee_contributions
WHERE {{ Candidate }}
    AND transaction_tp_code IN ('24A', '24N')
GROUP BY cand_recipient;