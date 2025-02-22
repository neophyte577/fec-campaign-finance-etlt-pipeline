WITH stg_individual_contributions AS (
    {{ union_tables('individual_contributions') }} 
),

report_types AS (
    SELECT * FROM {{ source('fec', 'report_types') }}
),

transaction_types AS (
    SELECT * FROM {{ source('fec', 'transaction_types') }}
)

SELECT
    ic.sub_id, -- PRIMARY KEY
    ic.cmte_id,
    r.report_type, -- FOREIGN KEY
    ic.transaction_pgi,
    t.transaction_type,
    ic.entity_tp,
    ic.name,
    ic.city,
    ic.state,
    LEFT(ic.zip_code, 5) AS zip_code,
    ic.employer,
    ic.occupation,
    ic.transaction_dt,
    ic.transaction_amt,
    CASE
        WHEN ic.amndt_ind = 'N' THEN 'New'
        WHEN ic.amndt_ind = 'A' THEN 'Amendment'
        WHEN ic.amndt_ind = 'T' THEN 'Termination'
        WHEN ic.amndt_ind = NULL THEN ic.amndt_ind
    END AS amndt_id
FROM stg_individual_contributions ic
INNER JOIN report_types r ON ic.rpt_tp = r.rpt_tp_code
INNER JOIN transaction_types t ON ic.transaction_tp = t.tran_tp_code