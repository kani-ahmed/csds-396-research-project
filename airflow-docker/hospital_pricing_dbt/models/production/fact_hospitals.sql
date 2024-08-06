-- models/production/fact_hospitals.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='hospital_id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    hospital_id,
    hospital,
    location_id
FROM {{ ref('hospitals') }}  -- Reference the 'hospitals' model in the warehouse directory
{% if is_incremental() %}
WHERE hospital_id > (SELECT COALESCE(MAX(hospital_id), 0) FROM {{ this }})
{% endif %}
