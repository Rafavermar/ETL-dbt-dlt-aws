{{ config(materialized='table') }}

SELECT
    cod_taric,
    nivel_taric,
    taric
FROM read_csv('/dbt_project/data/bronze/datacomex/metadata/taric.csv')
