SELECT COUNT(*)
FROM {{ ref('agg_indiv_contr_by_cand') }}
WHERE total_contributions < 0