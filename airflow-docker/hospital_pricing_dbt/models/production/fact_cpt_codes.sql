-- models/production/fact_cpt_codes.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='cpt_id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    cpt_id,
    cpt_code,
    description
FROM {{ ref('cpt_codes') }}
{% if is_incremental() %}
WHERE cpt_id > (SELECT COALESCE(MAX(cpt_id), 0) FROM {{ this }})
{% endif %}
