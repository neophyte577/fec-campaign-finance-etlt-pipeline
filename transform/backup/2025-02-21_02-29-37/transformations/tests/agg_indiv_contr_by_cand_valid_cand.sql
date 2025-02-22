SELECT COUNT(*)
FROM {{ ref('agg_indiv_contr_by_cand') }} AS agg
WHERE cand_name NOT IN (SELECT cand_name FROM {{ source('fec', 'candidate_master') }})