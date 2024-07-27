{{ config(materialized='table') }}

SELECT
    cod_pais,
    pais
FROM read_csv('/dbt_project/data/bronze/datacomex/metadata/paises.csv')
