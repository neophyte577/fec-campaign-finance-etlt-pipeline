with candidates as (
    SELECT * FROM {{ ref('stg_candidates') }}
    WHERE cand_election_yr = 2024
),

cand_comm_linkage as (
    SELECT * FROM {{ source('fec', 'cand_comm_linkage') }}
    WHERE cand_election_yr = 2024
),

individual_contributions as (
    SELECT * FROM {{ ref('stg_individual_contributions') }}
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
INNER JOIN cand_comm_linkage ccl ON cs.cand_id = ccl.cand_id
INNER JOIN individual_contributions ics on ccl.cmte_id = ics.cmte_id
WHERE cs.cand_office IN ('H', 'S', 'P') 
GROUP BY cs.cand_name, cs.party_code_desc, cs.cand_office_st, cs.cand_office, cs.cand_office_district;