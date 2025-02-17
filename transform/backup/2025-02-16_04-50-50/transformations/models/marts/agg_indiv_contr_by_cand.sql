with candidates as (
    SELECT * FROM {{ ref('stg_candidates') }}
    WHERE cand_election_yr = 2024
),

ccl_source as (
    SELECT * FROM {{ source('fec', 'cand_comm_linkage') }}
    WHERE cand_election_yr = 2024
),

indiv_cont_source as (
    SELECT * FROM {{ ref('src_indiv_contributions') }}
)

SELECT
    cs.cand_name,
    cs.party_code_desc,
    cs.cand_office_st,
    CASE
        WHEN cs.cand_office = 'H' THEN 'House'
        WHEN cs.cand_office = 'S' THEN 'Senate'
        WHEN cs.cand_office = 'P' THEN 'President'
        ELSE cs.cand_office
    END as cand_office,
    cs.cand_office_district,
    SUM(ics.transaction_amt) as total_contributions
FROM candidates cs
INNER JOIN ccl_source ccl ON cs.cand_id = ccl.cand_id
INNER JOIN indiv_cont_source ics on ccl.cmte_id = ics.cmte_id
WHERE cs.cand_office IN ('H', 'S', 'P') 
GROUP BY cs.cand_name, cs.party_code_desc, cs.cand_office_st, cs.cand_office, cs.cand_office_district;