SELECT SUM(total_contributions) AS total, cand_name AS name FROM FEC.transformed.agg_indiv_contr_by_cand
GROUP BY name
ORDER BY total DESC 
LIMIT 10;