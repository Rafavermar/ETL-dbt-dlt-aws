-- models/03.enriched/enriched_comex.sql
{{ config(materialized='view') }}

SELECT
    *,
    CAST(euros AS DOUBLE) / CAST(kilogramos AS DOUBLE) AS euros_per_kg,
    CAST(dolares AS DOUBLE) / CAST(kilogramos AS DOUBLE) AS dollars_per_kg
FROM {{ ref('cleaned_comex') }}
