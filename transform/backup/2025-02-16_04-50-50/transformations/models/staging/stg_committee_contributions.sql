with stg_committee_contributions AS (
    SELECT * FROM {{ source('fec', 'committee_contributions') }}
),

report_typ_source AS (
    SELECT * FROM {{ source('fec', 'report_types') }}
),

transaction_typ_source AS (
    SELECT * FROM {{ source('fec', 'transaction_types') }}
)

SELECT
    cc.sub_id, -- PRIMARY KEY
    cc.cmte_id,
    cc.transaction_pgi,
    cc.entity_tp,
    cc.cand_id,
    cc.name,
    cc.city,
    cc.state,
    cc.zip_code,
    cc.employer,
    cc.occupation,
    r.report_type, -- FOREIGN KEY
    t.transaction_type, -- FOREIGN KEY
    cc.transaction_dt,
    cc.transaction_amt,
    cc.other_id,
    CASE
        WHEN cc.amndt_ind = 'N' THEN 'New'
        WHEN cc.amndt_ind = 'A' THEN 'Amendment'
        WHEN cc.amndt_ind = 'T' THEN 'Termination'
        ELSE amndt_ind
    END AS amndt_id,
FROM stg_committee_contributions cc
INNER JOIN report_typ_source r ON cc.rpt_tp = r.rpt_tp_code 
INNER JOIN transaction_typ_source t ON cc.transaction_tp = t.tran_tp_code 