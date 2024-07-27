-- models/02.cleaned/cleaned_comex.sql
{{ config(materialized='view') }}

SELECT
    flujo,
    año,
    mes,
    estado,
    (SELECT pais FROM {{ ref('dim_paises') }} WHERE cod_pais = raw_comex.pais) AS pais,
    (SELECT provincia FROM {{ ref('dim_provincias') }} WHERE cod_provincia = raw_comex.provincia) AS provincia,
    CAST(euros AS DOUBLE) AS euros,
    CAST(dolares AS DOUBLE) AS dolares,
    nivel_taric,
    (SELECT taric FROM {{ ref('dim_taric') }} WHERE cod_taric = raw_comex.cod_taric) AS taric,
    raw_comex.cod_taric,
    CAST(kilogramos AS DOUBLE) AS kilogramos
FROM {{ ref('stg_comex') }} raw_comex
WHERE taric IS NOT NULL
