SELECT SUM(transaction_amt)
FROM FEC.TRANSFORMED.fct_committee_contributions
WHERE {{ Candidate }}
    AND transaction_tp_code = '24A'
GROUP BY cand_recipient;