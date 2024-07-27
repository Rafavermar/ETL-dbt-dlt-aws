-- models/05.datamart/fact_comex.sql
{{ config(materialized='table') }}

SELECT
    flujo,
    año,
    mes,
    estado,
    pais,
    provincia,
    taric,
    cod_taric,
    SUM(euros) AS total_euros,
    SUM(dolares) AS total_dollars,
    SUM(kilogramos) AS total_kg,
    AVG(euros_per_kg) AS avg_euros_per_kg,
    AVG(dollars_per_kg) AS avg_dollars_per_kg
FROM {{ ref('formatted_comex') }}
GROUP BY
    flujo,
    año,
    mes,
    estado,
    pais,
    provincia,
    taric,
    cod_taric
