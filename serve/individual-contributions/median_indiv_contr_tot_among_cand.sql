SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY total_contributions) AS median
FROM FEC.TRANSFORMED.agg_indiv_contr_by_cand;