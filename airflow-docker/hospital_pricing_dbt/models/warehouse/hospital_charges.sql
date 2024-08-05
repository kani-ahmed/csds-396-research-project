{{ config(
    materialized='table',
    schema='warehouse'
) }}

WITH stg_data AS (
    SELECT *
    FROM {{ ref('stg_hospital_charges_cleaned') }}
)

SELECT
    stg.id,
    cpt.cpt_id,
    p.payer_id,
    c.city_id,
    z.zipcode_id,
    s.state_id,
    h.hospital_id,
    stg.cash_discount,
    stg.deidentified_max_allowed,
    stg.deidentified_min_allowed,
    stg.description,
    stg.gross_charge,
    stg.payer_allowed_amount
FROM stg_data stg
LEFT JOIN {{ ref('dim_cpt_codes') }} cpt ON stg.cpt_code = cpt.cpt_code
LEFT JOIN {{ ref('dim_payers') }} p ON stg.payer_id = p.payer
LEFT JOIN {{ ref('dim_cities') }} c ON stg.city_id = c.city
LEFT JOIN {{ ref('dim_zipcodes') }} z ON stg.zipcode_id = z.zipcode
LEFT JOIN {{ ref('dim_states') }} s ON stg.state_id = s.state
LEFT JOIN {{ ref('dim_hospitals') }} h ON stg.hospital_id = h.hospital