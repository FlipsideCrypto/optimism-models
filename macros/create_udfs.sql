{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
    {{ create_js_hex_to_int() }};
    {{ create_udf_hex_to_int(
            schema = "public"
        ) }}

        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
