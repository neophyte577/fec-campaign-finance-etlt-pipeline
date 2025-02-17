with stg_committees as (
    SELECT * FROM {{ source('election', 'committee_master') }}
),

committee_types as (
    SELECT * FROM {{ source('election', 'committee_types') }}
),

parties as (
    SELECT * FROM {{ source('election', 'parties') }}
)

SELECT
    c.cmte_id, -- PRIMARY KEY
    c.committee_type, -- FOREIGN KEY
    c.cmte_nm,
    c.cmte_city,
    c.cmte_zip,
    c.connected_org_nm,
    c.cand_id,
    ct.cmte_typ,
    p.party_code_desc,
    CASE
        WHEN c.cmte_dsgn = 'A' THEN 'Authorized by Candidate'
        WHEN c.cmte_dsgn = 'B' THEN 'Lobbyist/Registrant PAC'
        WHEN c.cmte_dsgn = 'D' THEN 'Leadership PAC'
        WHEN c.cmte_dsgn = 'J' THEN 'Joint Fundrasier'
        WHEN c.cmte_dsgn = 'P' THEN 'Principal Campaign Committee of Candidate'
        WHEN c.cmte_dsgn = 'U' THEN 'Unauthorized'
        ELSE c.cmte_dsgn
    END as cmte_dsgn,
    CASE
        WHEN c.org_tp = 'C' THEN 'Corporation'
        WHEN c.org_tp = 'L' THEN 'Membership Organization'
        WHEN c.org_tp = 'T' THEN 'Trade Association'
        WHEN c.org_tp = 'V' THEN 'Cooperative'
        WHEN c.org_tp = 'W' THEN 'Corporation Without Capital Stock'
        ELSE c.org_tp
    END as org_tp
FROM stg_committees c
INNER JOIN committee_types ct ON c.cmte_tp = ct.cmte_tp_code
LEFT JOIN parties p ON c.cmte_pty_affiliation = p.party_code