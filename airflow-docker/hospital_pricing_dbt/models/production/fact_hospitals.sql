-- models/production/fact_hospitals.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='hospital_id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    h.hospital_id,
    h.hospital_name,
    h.location_id
FROM {{ ref('hospitals') }} h
JOIN {{ ref('fact_locations') }} fl ON h.location_id = fl.location_id
{% if is_incremental() %}
WHERE h.hospital_id > (SELECT COALESCE(MAX(hospital_id), 0) FROM {{ this }})
{% endif %}