{{ config(materialized='table') }}

SELECT
    cod_provincia,
    provincia,
    cod_comunidad,
    comunidad
FROM read_csv('/dbt_project/data/bronze/datacomex/metadata/provincias.csv')
