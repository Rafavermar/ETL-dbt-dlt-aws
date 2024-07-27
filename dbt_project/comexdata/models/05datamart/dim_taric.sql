-- models/05.datamart/dim_taric.sql
{{ config(materialized='table') }}

SELECT
    cod_taric,
    nivel_taric,
    taric
FROM {{ ref('stg_taric') }}
