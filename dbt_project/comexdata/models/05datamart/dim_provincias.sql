-- models/05.datamart/dim_provincias.sql
{{ config(materialized='table') }}

SELECT
    cod_provincia,
    provincia,
    cod_comunidad,
    comunidad
FROM {{ ref('stg_provincias') }}
