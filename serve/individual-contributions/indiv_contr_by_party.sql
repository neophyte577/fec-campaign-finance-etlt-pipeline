SELECT SUM(total_contributions), party 
FROM FEC.transformed.agg_indiv_contr_by_cand
GROUP BY party;
