WITH committee_contributions AS (
    SELECT * FROM {{ ref('stg_committee_contributions') }}
    WHERE CAND_ID IS NOT NULL
),

candidates AS (
    SELECT * FROM {{ ref('stg_candidates') }}
),

committees AS (
    SELECT * FROM {{ ref('stg_committees') }}
)

SELECT
    cc.sub_id, -- PRIMARY KEY
    cc.cmte_id,
    cmte.cmte_nm,
    cc.amndt_id,
    cc.report_type,
    cc.transaction_type,
    cand.cand_name AS cand_recipient,
    cc.city AS cand_city,
    cc.state AS cand_state,
    cc.zip_code AS cand_zip_code,
    cc.transaction_dt,
    cc.transaction_amt
FROM committee_contributions cc
INNER JOIN candidates cand ON cc.cand_id = cand.cand_id
INNER JOIN committees cmte ON cc.cmte_id = cmte.cmte_id
WHERE cc.amndt_id != 'Termination'
