WITH stg_candidates AS (
    {{ union_tables('candidate_master') }} 
),    

parties AS (
    SELECT * FROM {{ source('fec', 'parties') }}
)

SELECT 
    c.cand_id, -- PRIMARY KEY
    c.cand_name,
    p.party, -- FOREIGN KEY
    c.cand_election_yr,
    c.cand_office_st,
    c.cand_office,
    c.cand_city,
    c.cand_st,
    CASE 
        WHEN cand_office_district IS NOT NULL 
        THEN cand_office_st || cand_office_district 
        ELSE NULL 
    END AS cand_office_district
    CASE 
        WHEN c.cand_ici = 'I' THEN 'Incumbent'
        WHEN c.cand_ici = 'C' THEN 'Challenger'
        WHEN c.cand_ici = 'O' THEN 'Open'
    END AS cand_ici,
    c.cand_status,
 FROM stg_candidates c
 INNER JOIN parties p ON c.cand_pty_affiliation = p.party_code -- FOREIGN KEY