-- models/05.datamart/dim_paises.sql
{{ config(materialized='table') }}

SELECT
    cod_pais,
    pais
FROM {{ ref('stg_paises') }}
