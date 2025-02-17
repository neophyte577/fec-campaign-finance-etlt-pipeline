WITH stg_operating_expenditures AS (
    {{ union_tables('operating_expenditures') }}
),

report_typ_source AS (
    SELECT * FROM {{ source('fec', 'report_types') }}
)

SELECT
    oe.sub_id, -- PRIMARY KEY
    oe.cmte_id,
    oe.rpt_yr,
    rt.rp_typ, -- FOREIGN KEY
    oe.name,
    oe.city,
    oe.state,
    oe.zip_code,
    oe.transaction_amt,
    oe.transaction_dt,
    oe.category_desc,
    oe.purpose,
    oe.memo_text,
    CASE
        WHEN oe.amndt_ind = 'N' THEN 'New'
        WHEN oe.amndt_ind = 'A' THEN 'Amendment'
        WHEN oe.amndt_ind = 'T' THEN 'Termination'
        ELSE oe.amndt_ind
    END AS amndt_id
FROM stg_operating_expenditures oe
INNER JOIN report_typ_source rt ON oe.rpt_tp = rt.rp_typ_code -- FOREIGN KEY