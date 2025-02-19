with candidates as (
    SELECT * FROM {{ ref('stg_candidates') }}
),

cand_comm_linkage as (
    {{ union_tables('cand_comm_linkage') }} 
),

individual_contributions as (
    SELECT * FROM {{ ref('stg_individual_contributions') }}
)

SELECT
    cs.cand_name,
    cs.party,
    cs.cand_office_st,
    CASE
        WHEN cs.cand_office = 'H' THEN 'House'
        WHEN cs.cand_office = 'S' THEN 'Senate'
        WHEN cs.cand_office = 'P' THEN 'President'
        ELSE cs.cand_office
    END as cand_office,
    cs.cand_office_district,
    SUM(ic.transaction_amt) as total_contributions
FROM candidates cs
INNER JOIN cand_comm_linkage ccl ON cs.cand_id = ccl.cand_id
INNER JOIN individual_contributions ic on ccl.cmte_id = ic.cmte_id
WHERE cs.cand_office IN ('H', 'S', 'P') 
    AND cs.cand_office_st IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC')
GROUP BY cs.cand_name, cs.party, cs.cand_office_st, cs.cand_office, cs.cand_office_district