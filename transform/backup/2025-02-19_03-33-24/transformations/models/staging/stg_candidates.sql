WITH stg_candidates AS (
    {{ union_tables('candidate_master') }} 
),    

parties AS (
    SELECT * FROM {{ source('fec', 'parties') }}
)

SELECT 
    cm.cand_id, -- PRIMARY KEY
    cm.cand_name,
    p.party, -- FOREIGN KEY
    cm.cand_election_yr,
    cm.cand_office_st,
    cm.cand_office,
    cm.cand_city,
    cm.cand_st,
    CASE 
        WHEN cand_office_district IS NOT NULL 
        THEN cand_office_st || cand_office_district 
        ELSE NULL 
    END AS cand_office_district,
    CASE 
        WHEN cm.cand_ici = 'I' THEN 'Incumbent'
        WHEN cm.cand_ici = 'C' THEN 'Challenger'
        WHEN cm.cand_ici = 'O' THEN 'Open'
    END AS cand_ici,
    cm.cand_status,
 FROM stg_candidates cm
 INNER JOIN parties p ON cm.cand_pty_affiliation = p.party_code -- FOREIGN KEY