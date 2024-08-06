-- models/production/fact_payers.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='payer_id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    payer_id,
    payer
FROM {{ ref('payers') }}
{% if is_incremental() %}
WHERE payer_id NOT IN (SELECT payer_id FROM {{ this }})
{% endif %}