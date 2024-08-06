-- models/production/fact_payer_locations.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    id,
    payer_id,
    location_id
FROM {{ ref('payer_locations') }}
{% if is_incremental() %}
WHERE id NOT IN (SELECT id FROM {{ this }})
{% endif %}