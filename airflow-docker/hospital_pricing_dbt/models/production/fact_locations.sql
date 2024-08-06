-- models/production/fact_locations.sql
{{ config(
    materialized='incremental',
    schema='production_db',
    unique_key='location_id',
    on_schema_change='sync_all_columns'
) }}

SELECT
    location_id,
    state,
    city,
    zipcode
FROM {{ ref('locations') }}  -- Reference the 'locations' model in the warehouse directory
{% if is_incremental() %}
WHERE location_id > (SELECT COALESCE(MAX(location_id), 0) FROM {{ this }})
{% endif %}
