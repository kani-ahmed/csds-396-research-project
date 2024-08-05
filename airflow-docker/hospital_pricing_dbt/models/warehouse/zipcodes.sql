{{ config(
    materialized='table',
    schema='warehouse'
) }}

SELECT * FROM {{ ref('dim_zipcodes') }}