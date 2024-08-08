{{ config(materialized='table', schema='warehouse') }}

SELECT
    hospital_id,
    hospital AS hospital_name,
    location_id
FROM {{ ref('dim_hospitals') }}