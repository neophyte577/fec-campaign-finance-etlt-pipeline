WITH operating_expenditures AS (
    SELECT * FROM {{ ref('stg_operating_expenditures') }}
),

committees AS (
    SELECT * FROM {{ ref('stg_committees') }}
)

SELECT
    oe.sub_id, -- PRIIMARY KEY
    oe.cmte_id,
    cmte.cmte_nm as cmte_name,
    oe.name,
    oe.city,
    oe.state,
    oe.purpose,
    oe.category_desc,
    oe.transaction_amt,
    oe.transaction_dt
FROM operating_expenditures oe
INNER JOIN committees cmte ON oe.cmte_id = cmte.cmte_id