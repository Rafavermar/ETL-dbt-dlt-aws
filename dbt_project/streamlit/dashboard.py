import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

pd.options.display.float_format = '{:.2f}'.format

# Configuración del layout de la página
st.set_page_config(layout="wide")

# Conexión a la base de datos
con = duckdb.connect(database='/dbt_project/dev.duckdb', read_only=True)

# Título
st.title("Análisis de Comercio Exterior")

# Selector de tipo de flujo
flujo = st.sidebar.selectbox("Selecciona el tipo de flujo", [("E", "Exportación"), ("I", "Importación")], format_func=lambda x: x[1])

# Selector de Código TARIC
taric_options = con.execute("SELECT DISTINCT cod_taric FROM dbt_blue.fact_comex ORDER BY cod_taric").fetchall()
selected_taric = st.sidebar.selectbox("Selecciona un código TARIC", [x[0] for x in taric_options])

# Rango de fechas
fechas_disponibles = con.execute("SELECT MIN(mes), MAX(mes) FROM dbt_blue.fact_comex WHERE año = 2024").fetchone()
mes_inicio, mes_fin = st.sidebar.select_slider("Selecciona el rango de meses", options=list(range(1, 13)), value=(fechas_disponibles[0], fechas_disponibles[1]))

# Consulta para obtener el top 5 de códigos TARIC
query_top5 = """
WITH RankedData AS (
    SELECT
        flujo,
        pais,
        CAST(año AS VARCHAR) AS año,
        mes,
        taric,
        cod_taric,
        total_euros/1e6 AS total_euros_millones,
        total_dollars/1e6 AS total_dollars_millones,
        total_kg/1000 AS total_kg_toneladas,
        RANK() OVER (PARTITION BY flujo, año, mes ORDER BY total_euros DESC) AS rank
    FROM dbt_blue.fact_comex
    WHERE flujo = ? AND mes BETWEEN ? AND ? AND año = 2024
    GROUP BY flujo, pais, año, mes, taric, cod_taric, total_euros, total_dollars, total_kg
)
SELECT flujo, pais, año, mes, taric, cod_taric, total_euros_millones, total_dollars_millones, total_kg_toneladas
FROM RankedData
WHERE rank <= 5
"""
df_top5 = con.execute(query_top5, (flujo[0], mes_inicio, mes_fin)).df()

# Formatear los datos para visualización
df_top5['flujo'] = df_top5['flujo'].map({'I': 'Importación', 'E': 'Exportación'})
df_top5['mes'] = df_top5['mes'].apply(lambda x: {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}.get(x, ''))

# Mostrar tabla Top 5
st.header("Top 5 de Códigos TARIC por Total de Euros (en millones) y mes")
st.dataframe(df_top5)

# Consulta para obtener detalles para el código TARIC seleccionado
query_details = """
SELECT
    CAST(año AS VARCHAR) AS año,
    mes,
    flujo,
    taric,
    SUM(total_euros)/1e6 AS total_euros_millones,
    SUM(total_dollars)/1e6 AS total_dollars_millones,
    SUM(total_kg)/1000 AS total_kg_toneladas
FROM dbt_blue.fact_comex
WHERE cod_taric = ?
GROUP BY año, mes, flujo, taric
ORDER BY año, mes
"""
df_details = con.execute(query_details, (selected_taric,)).df()
df_details['flujo'] = df_details['flujo'].map({'I': 'Importación', 'E': 'Exportación'})
df_details['mes'] = df_details['mes'].apply(lambda x: {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}.get(x, ''))


# Mostrar detalles para el código TARIC seleccionado
st.header(f"Detalles para el código TARIC {selected_taric}")
st.dataframe(df_details)


# Consulta para obtener tendencias mensuales
query_trends = """
SELECT
    mes,
    SUM(total_euros)/1e6 AS total_euros_millions,
    SUM(total_dollars)/1e6 AS total_dollars_millions,
    SUM(total_kg)/1000 AS total_kg_tons
FROM dbt_blue.fact_comex
WHERE año = 2024 AND flujo = ?
GROUP BY mes
ORDER BY mes
"""
df_trends = con.execute(query_trends, (flujo[0],)).df()

# Gráfico de tendencias mensuales
fig, ax = plt.subplots(figsize=(10, 5))
ax.set_facecolor('black')
fig.patch.set_facecolor('black')
ax.plot(df_trends['mes'], df_trends['total_euros_millions'], marker='o', linestyle='-', color='white', label='Total Euros (Millones)')
ax.plot(df_trends['mes'], df_trends['total_dollars_millions'], marker='o', linestyle='-', color='yellow', label='Total Dollars (Millones)')
ax.set_title('Evolución Mensual del Comercio por Código TARIC', color='white')
ax.set_xlabel('Mes', color='white')
ax.set_ylabel('Total Moneda (Millones)', color='white')
ax.tick_params(axis='x', colors='white')
ax.tick_params(axis='y', colors='white')
ax.spines['top'].set_color('white')
ax.spines['bottom'].set_color('white')
ax.spines['left'].set_color('white')
ax.spines['right'].set_color('white')
ax.legend()
st.pyplot(fig)
