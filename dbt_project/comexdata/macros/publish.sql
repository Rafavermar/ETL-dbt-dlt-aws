-- macros/publish_macro.sql

{% macro publish() %}
    {% set sql %}
        CREATE OR REPLACE TABLE dbt_green.fact_bike_trips AS SELECT * FROM dbt_blue.fact_bike_trips;
    {% endset %}

    {%- if target.name == 'green' -%}
        {% do log("Merging into green environment...", info=True) %}
        {% do run_query(sql) %}
        {% do log("Merge into green environment done", info=True) %}
    {%- else -%}
        {% do exceptions.warn("[WARNING]: Target schema is not green") %}
    {%- endif -%}
{% endmacro %}
