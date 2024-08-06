{{ config(
    materialized='table',
    schema='clean_data'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY cpt_code) AS cpt_id,
    cpt_code,
    NULL AS description
FROM (
    SELECT DISTINCT cpt_code
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE cpt_code IS NOT NULL
) AS distinct_cpt_codes