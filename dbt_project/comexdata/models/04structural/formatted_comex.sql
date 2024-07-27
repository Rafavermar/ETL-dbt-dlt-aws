-- models/04.structural/formatted_comex.sql
{{ config(materialized='view') }}

SELECT
    flujo,
    año,
    mes,
    estado,
    pais,
    provincia,
    euros,
    dolares,
    nivel_taric,
    taric,
    cod_taric,
    kilogramos,
    euros_per_kg,
    dollars_per_kg
FROM {{ ref('enriched_comex') }}
