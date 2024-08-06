{{ config(materialized='table', schema='warehouse') }}

SELECT
    hospital_id,
    hospital,
    location_id
FROM {{ ref('dim_hospitals') }}