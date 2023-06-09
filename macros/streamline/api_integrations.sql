{% macro create_aws_optimism_api() %}
    {{ log("Creating integration for target:" ~ target) }}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_optimism_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-optimism' api_allowed_prefixes = (
            '<PROD_URL_PLACEHOLDER>'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_optimism_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-optimism' api_allowed_prefixes = (
            'https://4sovbxzgsf.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "sbx" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_optimism_api_sbx_shah api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::579011195466:role/snowflake-api-optimism' api_allowed_prefixes = (
            'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
