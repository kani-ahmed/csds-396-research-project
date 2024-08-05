{{ config(
    materialized='table',
    schema='intermediate_cleaning'
) }}

WITH RECURSIVE numbers(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 10
),

source_data AS (
    SELECT *
    FROM {{ source('staging_db', 'staging_table') }}
),

split_cpt_codes AS (
    SELECT
        id AS original_id,
        TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(cpt_code, ',', numbers.n), ',', -1)) AS cpt_code,
        cash_discount,
        deidentified_max_allowed,
        deidentified_min_allowed,
        TRIM(payer_id) AS payer_id,
        TRIM(city_id) AS city_id,
        TRIM(zipcode_id) AS zipcode_id,
        TRIM(hospital_id) AS hospital_id,
        TRIM(state_id) AS state_id,
        TRIM(description) AS description,
        gross_charge,
        payer_allowed_amount
    FROM source_data
    INNER JOIN numbers
    ON CHAR_LENGTH(cpt_code) - CHAR_LENGTH(REPLACE(cpt_code, ',', '')) >= numbers.n - 1
),

cleaned_data AS (
    SELECT
        original_id,
        cpt_code,
        cash_discount,
        deidentified_max_allowed,
        deidentified_min_allowed,
        payer_id,
        city_id,
        zipcode_id,
        hospital_id,
        state_id,
        description,
        gross_charge,
        payer_allowed_amount
    FROM split_cpt_codes
    WHERE cpt_code REGEXP '^[0-9]{5}$'
),

final_data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY original_id, cpt_code) AS id,
        original_id,
        cpt_code,
        cash_discount,
        deidentified_max_allowed,
        deidentified_min_allowed,
        payer_id,
        city_id,
        zipcode_id,
        hospital_id,
        state_id,
        description,
        gross_charge,
        payer_allowed_amount
    FROM cleaned_data
)

SELECT
    id,
    original_id,
    cpt_code,
    cash_discount,
    deidentified_max_allowed,
    deidentified_min_allowed,
    payer_id,
    city_id,
    zipcode_id,
    hospital_id,
    state_id,
    description,
    gross_charge,
    payer_allowed_amount
FROM final_data