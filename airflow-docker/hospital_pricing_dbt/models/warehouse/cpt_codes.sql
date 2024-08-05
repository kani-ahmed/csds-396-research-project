{{ config(
    materialized='table',
    schema='warehouse'
) }}

SELECT * FROM {{ ref('dim_cpt_codes') }}