SELECT cand_name, cand_office, cand_office_st, party, SUM(total_contributions) AS total
FROM FEC.transformed.agg_indiv_contr_by_cand
GROUP BY cand_name
ORDER BY total DESC 
LIMIT 100;