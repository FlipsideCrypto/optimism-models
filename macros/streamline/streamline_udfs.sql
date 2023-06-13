{% macro create_udf_get_chainhead() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_avalanche_api AS {% if target.name == "prod" %}
        'https://s7qxto6wkd.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = aws_terra_api AS {% if target.name == "prod" %}
        'https://s7qxto6wkd.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_decode_logs() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_decode_logs(
        json OBJECT
    ) returns ARRAY api_integration = aws_arbitrum_api AS {% if target.name == "prod" %}
        'https://s7qxto6wkd.execute-api.us-east-1.amazonaws.com/prod/bulk_decode_logs'
    {% else %}
        'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/bulk_decode_logs'
    {%- endif %};
{% endmacro %}