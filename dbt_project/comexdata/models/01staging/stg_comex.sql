{{ config(materialized='table') }}

WITH comex_data AS (
  SELECT * FROM read_csv('/dbt_project/data/bronze/datacomex/comex_taric/202401/comex_taric_202401.csv')
  UNION ALL
  SELECT * FROM read_csv('/dbt_project/data/bronze/datacomex/comex_taric/202402/comex_taric_202402.csv')
  UNION ALL
  SELECT * FROM read_csv('/dbt_project/data/bronze/datacomex/comex_taric/202403/comex_taric_202403.csv')
    UNION ALL
  SELECT * FROM read_csv('/dbt_project/data/bronze/datacomex/comex_taric/202404/comex_taric_202404.csv')
)

SELECT
    flujo,
    año,
    mes,
    estado,
    pais,
    provincia,
    CAST(REPLACE(euros, ',', '') AS DOUBLE) AS euros,
    CAST(REPLACE(dolares, ',', '') AS DOUBLE) AS dolares,
    nivel_taric,
    CAST(cod_taric AS VARCHAR) AS cod_taric,
    CAST(REPLACE(kilogramos, ',', '') AS DOUBLE) AS kilogramos
FROM comex_data
