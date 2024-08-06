{{ config(materialized='table', schema='warehouse') }}

WITH normalized_payers AS (
    SELECT * FROM {{ ref('payers') }}
),
payer_locations_with_id AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY np.payer_id, h.location_id) AS id,
        np.payer_id,
        h.location_id
    FROM {{ ref('stg_hospital_charges_cleaned') }} s
    JOIN {{ ref('dim_payers') }} dp ON s.payer_id = dp.payer
    JOIN normalized_payers np ON dp.payer = np.payer
    JOIN {{ ref('dim_hospitals') }} h ON s.hospital_id = h.hospital
)
SELECT * FROM payer_locations_with_id