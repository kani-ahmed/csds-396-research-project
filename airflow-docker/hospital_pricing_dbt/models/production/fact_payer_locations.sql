-- models/production/fact_payer_locations.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    pl.id,
    pl.payer_id,
    pl.location_id
FROM {{ ref('payer_locations') }} pl
JOIN {{ ref('fact_payers') }} fp ON pl.payer_id = fp.payer_id
JOIN {{ ref('fact_locations') }} fl ON pl.location_id = fl.location_id
{% if is_incremental() %}
WHERE pl.id NOT IN (SELECT id FROM {{ this }})
{% endif %}