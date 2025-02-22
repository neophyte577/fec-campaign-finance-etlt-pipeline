SELECT COUNT(*)
FROM (
    SELECT cand_name, COUNT(*)
    FROM {{ ref('agg_indiv_contr_by_cand') }}
    GROUP BY cand_name
    HAVING COUNT(*) > 1
) AS duplicates