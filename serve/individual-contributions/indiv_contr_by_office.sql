SELECT SUM(total_contributions), cand_office 
FROM FEC.transformed.agg_indiv_contr_by_cand
GROUP BY cand_office