{{ config(materialized='table', schema='warehouse') }}

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY payer) AS payer_id,
    payer
FROM {{ ref('dim_payers') }}