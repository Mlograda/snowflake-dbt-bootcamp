{{ config(
    materialized='view'
    )
}}

WITH src_hosts AS (
    SELECT * FROM {{ ref('src_hosts') }}
)
SELECT 
    host_id,
    NVL(host_name, 'Anonymous') AS host_name, -- replace null with Anonymous if null otherwise host_name
    IS_SUPERHOST,
    CREATED_AT,
    UPDATED_AT
FROM 
    src_hosts