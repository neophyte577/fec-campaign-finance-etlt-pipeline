WITH stg_committee_transactions as (
    {{ union_tables('committee_transactions') }} 
),

report_types as (
    SELECT * FROM {{ source('fec', 'report_types') }}
),

transaction_types as (
    SELECT * FROM {{ source('fec', 'transaction_types') }}
)

SELECT
    ct.sub_id, -- PRIMARY KEY
    r.report_type, -- FOREIGN KEY
    t.transaction_type,
    ct.cmte_id,
    ct.transaction_pgi,
    ct.entity_tp,
    ct.name,
    ct.city,
    ct.state,
    ct.zip_code,
    ct.employer,
    ct.occupation,
    ct.transaction_dt,
    ct.transaction_amt,
    ct.other_id,
    ct.memo_text,
    CASE
        WHEN ct.amndt_ind = 'N' THEN 'New'
        WHEN ct.amndt_ind = 'A' THEN 'Amendment'
        WHEN ct.amndt_ind = 'T' THEN 'Termination'
        WHEN ct.amndt_ind IS NULL THEN NULL
        ELSE amndt_ind
    END as amndt_ind,
FROM stg_committee_transactions ct
INNER JOIN report_types r ON ct.rpt_tp = r.rpt_tp_code
INNER JOIN transaction_types t ON ct.transaction_tp = t.tran_tp_code