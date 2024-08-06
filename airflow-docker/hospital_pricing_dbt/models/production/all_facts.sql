{{ config(materialized='table') }}

-- This model doesn't actually do anything, it just ensures proper run order
SELECT 1 as dummy_column
FROM {{ ref('fact_locations') }}
CROSS JOIN {{ ref('fact_payers') }}
CROSS JOIN {{ ref('fact_cpt_codes') }}
CROSS JOIN {{ ref('fact_hospitals') }}
CROSS JOIN {{ ref('fact_payer_locations') }}
CROSS JOIN {{ ref('fact_hospital_charges') }}
WHERE 1=0