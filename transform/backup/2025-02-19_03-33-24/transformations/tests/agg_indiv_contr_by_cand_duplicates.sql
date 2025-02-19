SELECT COUNT(*)
FROM (
    SELECT cand_name, cmte_id, transaction_dt, COUNT(*)
    FROM {{ ref('agg_indiv_contr_by_cand') }}
    GROUP BY cand_name, cmte_id, transaction_dt
    HAVING COUNT(*) > 1
) AS duplicates