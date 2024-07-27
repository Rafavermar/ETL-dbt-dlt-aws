WITH monthly_data AS (
    SELECT
        año,
        mes,
        flujo,
        SUM(total_euros) AS total_euros,  -- Usando la columna total_euros
        SUM(total_dollars) AS total_dollars,  -- Usando la columna total_dollars
        SUM(total_kg) AS total_kg
    FROM {{ ref('fact_comex') }}
    GROUP BY 1, 2, 3
)

SELECT
    año,
    mes,
    flujo,
    total_euros,
    total_dollars,
    total_kg,
    RANK() OVER (PARTITION BY flujo ORDER BY total_euros DESC) AS rank_by_euros
FROM monthly_data
ORDER BY año, mes, flujo