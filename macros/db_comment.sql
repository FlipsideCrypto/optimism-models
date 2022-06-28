{% macro db_comment() %}
    {% set query %}
SELECT
    TO_DATE(MIN(block_timestamp))
FROM
    silver.blocks {% endset %}
    {% set results = run_query(query) %}
    {% set results_list = results.columns [0].values() [0].strftime('%Y-%m-%d') %}
    {% set sql %}
    COMMENT
    ON database optimism IS 'Lite Mode dataset with recent data only. Min block_timestamp: {{ results_list }} 🌱 ' {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
