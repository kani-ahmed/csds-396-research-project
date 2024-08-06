{{ config(materialized='table', schema='clean_data') }}

WITH hospital_data AS (
    SELECT DISTINCT
        hospital_id AS hospital,
        state_id,
        city_id,
        zipcode_id
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE hospital_id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY h.hospital) AS hospital_id,
    h.hospital,
    l.location_id
FROM hospital_data h
LEFT JOIN {{ ref('dim_locations') }} l
    ON h.state_id = l.state
    AND h.city_id = l.city
    AND h.zipcode_id = l.zipcode