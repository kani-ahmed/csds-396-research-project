-- models/production/fact_hospital_charges.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    hc.id,
    hc.cpt_id,
    hc.payer_id,
    hc.hospital_id,
    hc.location_id,
    hc.cash_discount,
    hc.deidentified_max_allowed,
    hc.deidentified_min_allowed,
    hc.description,
    hc.gross_charge,
    hc.payer_allowed_amount
FROM {{ ref('stg_hospital_charges') }} hc
JOIN {{ ref('dim_cpt_codes') }} dc ON hc.cpt_id = dc.cpt_id
JOIN {{ ref('dim_payers') }} dp ON hc.payer_id = dp.payer_id
JOIN {{ ref('dim_hospitals') }} dh ON hc.hospital_id = dh.hospital_id
JOIN {{ ref('dim_locations') }} dl ON hc.location_id = dl.location_id
{% if is_incremental() %}
WHERE hc.id > (SELECT COALESCE(MAX(id), 0) FROM {{ this }})
{% endif %}