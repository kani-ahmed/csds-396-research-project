-- models/production/fact_hospital_charges.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    id,
    cpt_id,
    payer_id,
    hospital_id,
    location_id,
    cash_discount,
    deidentified_max_allowed,
    deidentified_min_allowed,
    description,
    gross_charge,
    payer_allowed_amount
FROM {{ ref('hospital_charges') }}  -- Reference the 'hospital_charges' model in the warehouse directory
{% if is_incremental() %}
WHERE id > (SELECT COALESCE(MAX(id), 0) FROM {{ this }})
{% endif %}
