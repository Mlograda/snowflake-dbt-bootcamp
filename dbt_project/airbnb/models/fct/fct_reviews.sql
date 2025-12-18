-- incremental materialization
{{ config(
    materialized='incremental',
    on_schema_change='fail' 
    ) 
}} --if the upstream schema changes , the dbt run command will fail incrementing the table

WITH src_reviews AS (
    SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %} -- JINJA helper, If 'True' then load only new rows
    AND review_date > (SELECT MAX(review_date) FROM {{ this }}) -- this is the current table ; fct_reviews.sql
{% endif %}