{{ config(materialized='table', schema='warehouse') }}

SELECT
    stg.id,
    cpt.cpt_id,
    p.payer_id,
    h.hospital_id,
    h.location_id,
    stg.cash_discount,
    stg.deidentified_max_allowed,
    stg.deidentified_min_allowed,
    stg.description,
    stg.gross_charge,
    stg.payer_allowed_amount
FROM {{ ref('stg_hospital_charges_cleaned') }} stg
LEFT JOIN {{ ref('dim_cpt_codes') }} cpt ON stg.cpt_code = cpt.cpt_code
LEFT JOIN {{ ref('dim_payers') }} p ON stg.payer_id = p.payer
LEFT JOIN {{ ref('dim_hospitals') }} h ON stg.hospital_id = h.hospital