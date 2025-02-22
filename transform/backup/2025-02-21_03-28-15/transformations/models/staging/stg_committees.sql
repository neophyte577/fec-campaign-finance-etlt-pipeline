WITH stg_committees as (
    {{ union_tables('committee_master') }} 
),

committee_types as (
    SELECT * FROM {{ source('fec', 'committee_types') }}
),

parties as (
    SELECT * FROM {{ source('fec', 'parties') }}
)

SELECT
    cm.cmte_id, -- PRIMARY KEY
    cm.cmte_nm,
    cm.cmte_city,
    cm.cmte_zip,
    cm.connected_org_nm,
    cm.cand_id,
    ct.committee_type, -- FOREIGN KEY
    p.party,
    CASE
        WHEN cm.cmte_dsgn = 'A' THEN 'Authorized by Candidate'
        WHEN cm.cmte_dsgn = 'B' THEN 'Lobbyist/Registrant PAC'
        WHEN cm.cmte_dsgn = 'D' THEN 'Leadership PAC'
        WHEN cm.cmte_dsgn = 'J' THEN 'Joint Fundrasier'
        WHEN cm.cmte_dsgn = 'P' THEN 'Principal Campaign Committee of Candidate'
        WHEN cm.cmte_dsgn = 'U' THEN 'Unauthorized'
        ELSE cm.cmte_dsgn
    END as cmte_dsgn,
    CASE
        WHEN cm.org_tp = 'C' THEN 'Corporation'
        WHEN cm.org_tp = 'L' THEN 'Membership Organization'
        WHEN cm.org_tp = 'T' THEN 'Trade Association'
        WHEN cm.org_tp = 'V' THEN 'Cooperative'
        WHEN cm.org_tp = 'W' THEN 'Corporation Without Capital Stock'
        ELSE cm.org_tp
    END as org_tp
FROM stg_committees cm
INNER JOIN committee_types ct ON cm.cmte_tp = ct.cmte_tp_code
LEFT JOIN parties p ON cm.cmte_pty_affiliation = p.party_code