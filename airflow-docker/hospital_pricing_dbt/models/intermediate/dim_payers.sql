{{ config(
    materialized='table',
    schema='clean_data'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY payer) AS payer_id,
    payer
FROM (
    SELECT DISTINCT payer_id AS payer
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE payer_id IS NOT NULL
) AS distinct_payers