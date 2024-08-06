{{ config(materialized='table', schema='clean_data') }}

WITH location_data AS (
    SELECT DISTINCT
        state_id AS state,
        city_id AS city,
        zipcode_id AS zipcode
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE state_id IS NOT NULL
      AND city_id IS NOT NULL
      AND zipcode_id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY state, city, zipcode) AS location_id,
    state,
    city,
    zipcode
FROM location_data