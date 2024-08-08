{{ config(
    materialized='table',
    schema='clean_data'
) }}

WITH distinct_cpt_codes AS (
    SELECT DISTINCT cpt_code
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE cpt_code IS NOT NULL
),
cpt_translations AS (
    SELECT
        CAST(`CPT Code` AS UNSIGNED) as cpt_code,
        Description as description
    FROM {{ source('intermediate_cleaning', 'cpt-translation') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY dc.cpt_code) AS cpt_id,
    dc.cpt_code,
    ct.description
FROM distinct_cpt_codes dc
LEFT JOIN cpt_translations ct ON dc.cpt_code = ct.cpt_code