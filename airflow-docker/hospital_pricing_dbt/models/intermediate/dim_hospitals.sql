{{ config(
    materialized='table',
    schema='clean_data'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY hospital) AS hospital_id,
    hospital
FROM (
    SELECT DISTINCT hospital_id AS hospital
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE hospital_id IS NOT NULL
) AS distinct_hospitals