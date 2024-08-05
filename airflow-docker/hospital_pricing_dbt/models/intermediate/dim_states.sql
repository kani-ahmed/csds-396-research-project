{{ config(
    materialized='table',
    schema='clean_data'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY state) AS state_id,
    state
FROM (
    SELECT DISTINCT state_id AS state
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE state_id IS NOT NULL
) AS distinct_states