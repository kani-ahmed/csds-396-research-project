{{ config(
    materialized='table',
    schema='clean_data'
) }}

WITH city_state_mapping AS (
    SELECT DISTINCT
        city_id AS city,
        state_id AS state
    FROM {{ ref('stg_hospital_charges_cleaned') }}
    WHERE city_id IS NOT NULL AND state_id IS NOT NULL
),

state_dimension AS (
    SELECT *
    FROM {{ ref('dim_states') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY csm.city) AS city_id,
    csm.city,
    sd.state_id
FROM city_state_mapping csm
LEFT JOIN state_dimension sd ON csm.state = sd.state