SELECT SUM(transaction_amt) AS total_amt, NAME FROM FEC.TRANSFORMED.STG_INDIVIDUAL_CONTRIBUTIONS
GROUP BY NAME
ORDER BY TOTAL_AMT DESC 
LIMIT 10;