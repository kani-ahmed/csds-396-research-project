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
FROM {{ ref('hospital_charges') }} hc
JOIN {{ ref('fact_cpt_codes') }} fc ON hc.cpt_id = fc.cpt_id
JOIN {{ ref('fact_payers') }} fp ON hc.payer_id = fp.payer_id
JOIN {{ ref('fact_hospitals') }} fh ON hc.hospital_id = fh.hospital_id
JOIN {{ ref('fact_locations') }} fl ON hc.location_id = fl.location_id
{% if is_incremental() %}
WHERE hc.id > (SELECT COALESCE(MAX(id), 0) FROM {{ this }})
{% endif %}