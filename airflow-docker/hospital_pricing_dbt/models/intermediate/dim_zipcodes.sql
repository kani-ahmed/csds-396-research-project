{{ config(
    materialized='table',
    schema='clean_data'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY zipcode) AS zipcode_id,
    zipcode
FROM (
    SELECT DISTINCT zipcode_id AS zipcode
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE zipcode_id IS NOT NULL
) AS distinct_zipcodes