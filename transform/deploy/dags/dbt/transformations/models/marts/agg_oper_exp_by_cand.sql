WITH operating_expenditures AS (
    SELECT * FROM {{ ref('stg_operating_expenditures') }}
),

committees AS (
    SELECT * FROM {{ ref('stg_committees') }}
),

candidates AS (
    SELECT * FROM {{ ref('stg_candidates') }}
)

SELECT
    oe.sub_id,
    oe.cmte_id,
    cmte.cmte_nm AS cmte_name,
    cand.cand_name,
    oe.name AS entity_name,
    oe.city,
    oe.state,
    oe.purpose,
    oe.category_desc,
    oe.transaction_amt,
    oe.transaction_dt
FROM operating_expenditures oe
LEFT JOIN committees cmte ON oe.cmte_id = cmte.cmte_id
INNER JOIN candidates cand ON cmte.cand_id = cand.cand_id
WHERE oe.state IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
                'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
                'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
                'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'US')