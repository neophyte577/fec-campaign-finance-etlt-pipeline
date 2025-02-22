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
    AND cc.state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'US')
