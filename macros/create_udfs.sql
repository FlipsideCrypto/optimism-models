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

        {{ create_udtf_get_base_table(
            schema = "streamline"
        ) }}
        {{ create_udf_get_chainhead() }}
        {{ create_udf_bulk_json_rpc() }}
        {{ create_udf_bulk_decode_logs() }}

        {% endset %}
        {% do run_query(sql) %}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}
