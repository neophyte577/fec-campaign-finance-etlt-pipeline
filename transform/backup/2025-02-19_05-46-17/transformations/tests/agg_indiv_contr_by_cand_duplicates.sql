SELECT COUNT(*)
FROM (
    SELECT cand_name, transaction_dt, COUNT(*)
    FROM {{ ref('agg_indiv_contr_by_cand') }}
    GROUP BY cand_name, transaction_dt
    HAVING COUNT(*) > 1
) AS duplicates