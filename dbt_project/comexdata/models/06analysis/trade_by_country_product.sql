WITH country_product_totals AS (
    SELECT
        pais,
        taric,
        cod_taric,
        flujo,
        SUM(total_euros) AS total_euros,  -- Usando la columna total_euros
        SUM(total_dollars) AS total_dollars,  -- Usando la columna total_dollars
        SUM(total_kg) AS total_weight,  -- Asumiendo que total_kg es correcto
        COUNT(*) AS transaction_count
    FROM {{ ref('fact_comex') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    pais,
    taric,
    cod_taric,
    flujo,
    total_euros,
    total_dollars,
    total_weight,
    transaction_count,
    RANK() OVER (PARTITION BY pais, flujo ORDER BY total_euros DESC) AS rank_by_value
FROM country_product_totals
ORDER BY pais, flujo, rank_by_value
