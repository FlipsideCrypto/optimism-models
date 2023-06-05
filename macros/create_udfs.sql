{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
{{ create_udf_keccak(
            schema = 'silver'
        ) }}
        {{ create_udf_simple_event_names(
            schema = 'silver'
        ) }}

        {% endset %}
        {% do run_query(sql) %}
        {% set name %}
        {{- fsc_utils.create_udfs() -}}
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
